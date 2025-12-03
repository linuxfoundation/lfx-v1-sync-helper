// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The v1-sync-helper service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// kvEntry implements a mock jetstream.KeyValueEntry interface for the handler
type kvEntry struct {
	key       string
	value     []byte
	operation jetstream.KeyValueOp
}

func (e *kvEntry) Key() string {
	return e.key
}

func (e *kvEntry) Value() []byte {
	return e.value
}

func (e *kvEntry) Operation() jetstream.KeyValueOp {
	return e.operation
}

func (e *kvEntry) Bucket() string {
	return "v1-objects"
}

func (e *kvEntry) Created() time.Time {
	return time.Now()
}

func (e *kvEntry) Delta() uint64 {
	return 0
}

func (e *kvEntry) Revision() uint64 {
	return 0
}

const (
	errKey            = "error"
	defaultListenPort = "8080"
	// gracefulShutdownSeconds should be higher than NATS client
	// request timeout, and lower than the pod or liveness probe's
	// terminationGracePeriodSeconds.
	gracefulShutdownSeconds = 25
	// natsQueue is commented out since WAL-listener handlers are disabled for now.
	//natsQueue = "dev.lfx.v1-sync-helper.queue"
)

var (
	logger     *slog.Logger
	cfg        *Config
	natsConn   *nats.Conn
	jsContext  jetstream.JetStream
	v1KV       jetstream.KeyValue
	mappingsKV jetstream.KeyValue
)

// main parses optional flags and starts the NATS subscribers.
func main() {
	// Load configuration
	var err error
	cfg, err = LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	var debug = flag.Bool("d", false, "enable debug logging")
	var port = flag.String("p", cfg.Port, "health checks port")
	var bind = flag.String("bind", cfg.Bind, "interface to bind on")

	flag.Usage = func() {
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()

	logOptions := &slog.HandlerOptions{}

	// Optional debug logging.
	if cfg.Debug || *debug {
		logOptions.Level = slog.LevelDebug
		logOptions.AddSource = true
	}

	logger = slog.New(slog.NewJSONHandler(os.Stdout, logOptions))
	slog.SetDefault(logger)

	// Support GET/POST monitoring "ping".
	http.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		// This always returns as long as the service is still running. As this
		// endpoint is expected to be used as a Kubernetes liveness check, this
		// service must likewise self-detect non-recoverable errors and
		// self-terminate.
		fmt.Fprintf(w, "OK\n")
	})

	// Basic health check.
	http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if natsConn == nil {
			http.Error(w, "no NATS connection", http.StatusServiceUnavailable)
			return
		}
		if !natsConn.IsConnected() || natsConn.IsDraining() {
			http.Error(w, "NATS connection not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "OK\n")
	})

	// Add an http listener for health checks. This server does NOT participate
	// in the graceful shutdown process; we want it to stay up until the process
	// is killed, to avoid liveness checks failing during the graceful shutdown.
	var addr string
	if *bind == "*" {
		addr = ":" + *port
	} else {
		addr = *bind + ":" + *port
	}
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logger.With(errKey, err).Error("http listener error")
			os.Exit(1)
		}
	}()

	// Create a wait group which is used to wait while draining (gracefully
	// closing) a connection.
	gracefulCloseWG := sync.WaitGroup{}

	// Support graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Initialize JWT client for v2 services
	if err := initJWTClient(cfg); err != nil {
		logger.With(errKey, err).Error("error initializing JWT client")
		os.Exit(1)
	}

	// Initialize Goa SDK clients for v2 services
	if err := initGoaClients(cfg); err != nil {
		logger.With(errKey, err).Error("error initializing Goa clients")
		os.Exit(1)
	}

	// Initialize v1 client for Auth0 authentication
	if err := initV1Client(cfg); err != nil {
		logger.With(errKey, err).Error("error initializing v1 client")
		os.Exit(1)
	}

	// Create NATS connection.
	gracefulCloseWG.Add(1)
	natsConn, err = nats.Connect(
		cfg.NATSURL,
		nats.DrainTimeout(gracefulShutdownSeconds*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, err error) {
			if s != nil {
				logger.With(errKey, err, "subject", s.Subject, "queue", s.Queue).Error("async NATS error")
			} else {
				logger.With(errKey, err).Error("async NATS error outside subscription")
			}
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			if ctx.Err() != nil {
				// If our parent background context has already been canceled, this is
				// a graceful shutdown. Decrement the wait group but do not exit, to
				// allow other graceful shutdown steps to complete.
				gracefulCloseWG.Done()
				return
			}
			// Otherwise, this handler means that max reconnect attempts have been
			// exhausted.
			logger.Error("NATS max-reconnects exhausted; connection closed")
			// Send a synthetic interrupt and give any graceful-shutdown tasks 5
			// seconds to clean up.
			done <- os.Interrupt
			time.Sleep(5 * time.Second)
			// Exit with an error instead of decrementing the wait group.
			os.Exit(1)
		}),
	)
	if err != nil {
		logger.With(errKey, err).Error("error creating NATS client")
		os.Exit(1)
	}

	// Create JetStream context
	jsContext, err = jetstream.New(natsConn)
	if err != nil {
		logger.With(errKey, err).Error("error creating JetStream context")
		os.Exit(1)
	}

	// Create KV bucket connections for v1 objects (from Meltano)
	v1KV, err = jsContext.KeyValue(ctx, "v1-objects")
	if err != nil {
		logger.With(errKey, err).Error("error accessing v1-objects KV bucket")
		os.Exit(1)
	}

	// Create v1 mappings KV bucket for storing v1 ID mappings
	mappingsKV, err = jsContext.KeyValue(ctx, "v1-mappings")
	if err != nil {
		logger.With(errKey, err).Error("error accessing v1-mappings KV bucket")
		os.Exit(1)
	}

	// Create or get the JetStream pull consumer for v1 objects KV bucket
	// This replaces the KV Watch() method to enable horizontal scaling
	consumerName := "v1-sync-helper-kv-consumer"
	streamName := "KV_v1-objects"

	consumer, err := jsContext.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:    consumerName,
		Durable: consumerName,
		// Uncomment when ready in the chart (see comments in charts/lfx-v1-sync-helper/values.yaml)
		// DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "$KV.v1-objects.>",
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
		Description:   "Pull consumer for v1-sync-helper to process v1-objects KV bucket changes with load balancing",
	})
	if err != nil {
		logger.With(errKey, err, "consumer", consumerName, "stream", streamName).Error("error creating JetStream pull consumer")
		os.Exit(1)
	}

	// Process KV updates using the JetStream pull consumer
	// Multiple instances will compete for messages automatically
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Fetch messages with a batch size of 1 and timeout
				msgs, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
				if err != nil {
					if err == jetstream.ErrNoMessages {
						// No messages available, continue polling
						continue
					}
					logger.With(errKey, err, "consumer", consumerName).Error("error fetching messages from consumer")
					continue
				}

				for msg := range msgs.Messages() {
					// Parse the message as a KV entry
					headers := msg.Headers()
					subject := msg.Subject()

					// Extract key from the subject ($KV.v1-objects.{key})
					key := ""
					if len(subject) > len("$KV.v1-objects.") {
						key = subject[len("$KV.v1-objects."):]
					}

					// Determine operation from headers
					operation := jetstream.KeyValuePut // Default to PUT
					if opHeader := headers.Get("KV-Operation"); opHeader != "" {
						switch opHeader {
						case "DEL":
							operation = jetstream.KeyValueDelete
						case "PURGE":
							operation = jetstream.KeyValuePurge
						}
					}

					// Create a mock KV entry for the handler
					entry := &kvEntry{
						key:       key,
						value:     msg.Data(),
						operation: operation,
					}

					// Process the KV entry
					kvHandler(entry)

					// Acknowledge the message
					if err := msg.Ack(); err != nil {
						logger.With(errKey, err, "key", key).Error("failed to acknowledge JetStream message")
					}
				}
			}
		}
	}()

	// WAL-listener handlers are commented out but kept for future use
	// when they need to write to the same v1-objects KV bucket
	/*
		// Define WAL-listener subjects locally since they're commented out in other files
		const (
			walProjectSubject       = "wal_listener.salesforce_project__c"
			walCollaborationSubject = "wal_listener.salesforce_collaboration__c"
		)

		// Subscribe to wal-listener v1 project events using JetStream
		walConsumer, err := jsContext.CreateOrUpdateConsumer(ctx, "wal", jetstream.ConsumerConfig{
			Durable:       "v1-sync-helper-wal-project",
			FilterSubject: walProjectSubject,
			DeliverGroup:  natsQueue,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		if err != nil {
			logger.With(errKey, err, "subject", walProjectSubject).Error("error creating consumer for WAL-listener subject")
			os.Exit(1)
		}
		if _, err = walConsumer.Consume(func(msg jetstream.Msg) {
			if err := msg.Ack(); err != nil {
				logger.With(errKey, err).Error("failed to acknowledge JetStream message")
				return
			}
			// walListenerHandler(&msg.Message, v1KV, mappingsKV) - handler commented out
		}); err != nil {
			logger.With(errKey, err, "subject", walProjectSubject).Error("error subscribing to WAL-listener subject")
			os.Exit(1)
		}

		// Subscribe to wal-listener v1 committee (collaboration) events using JetStream
		walCollabConsumer, err := jsContext.CreateOrUpdateConsumer(ctx, "wal", jetstream.ConsumerConfig{
			Durable:       "v1-sync-helper-wal-collab",
			FilterSubject: walCollaborationSubject,
			DeliverGroup:  natsQueue,
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		})
		if err != nil {
			logger.With(errKey, err, "subject", walCollaborationSubject).Error("error creating consumer for WAL-listener collaboration subject")
			os.Exit(1)
		}
		if _, err = walCollabConsumer.Consume(func(msg jetstream.Msg) {
			if err := msg.Ack(); err != nil {
				logger.With(errKey, err).Error("failed to acknowledge JetStream message")
				return
			}
			// walCollaborationHandler(&msg.Message, v1KV, mappingsKV) - handler commented out
		}); err != nil {
			logger.With(errKey, err, "subject", walCollaborationSubject).Error("error subscribing to WAL-listener collaboration subject")
			os.Exit(1)
		}
	*/

	// This next line blocks until SIGINT or SIGTERM is received, or NATS disconnects.
	<-done

	// Cancel the background context.
	cancel()

	// Drain the connection, which will drain all subscriptions, then close the
	// connection when complete.
	if !natsConn.IsClosed() && !natsConn.IsDraining() {
		logger.Info("draining NATS connections")
		if err := natsConn.Drain(); err != nil {
			logger.With(errKey, err).Error("error draining NATS connection")
			os.Exit(1)
		}
	}

	// Wait for the graceful shutdown steps to complete.
	gracefulCloseWG.Wait()

	// Immediately close the HTTP server after graceful shutdown has finished.
	if err = httpServer.Close(); err != nil {
		logger.With(errKey, err).Error("http listener error on close")
	}
}
