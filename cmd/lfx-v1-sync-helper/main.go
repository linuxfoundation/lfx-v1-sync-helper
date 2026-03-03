// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
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

const (
	errKey            = "error"
	defaultListenPort = "8080"
	natsQueue         = "lfx.v1-sync-helper.queue"
	lookupSubject     = "lfx.lookup_v1_mapping"
	// gracefulShutdownSeconds should be higher than NATS client
	// request timeout, and lower than the pod or liveness probe's
	// terminationGracePeriodSeconds.
	gracefulShutdownSeconds = 25
)

var (
	logger     *slog.Logger
	cfg        *Config
	natsConn   *nats.Conn
	jsContext  jetstream.JetStream
	v1KV       jetstream.KeyValue
	mappingsKV jetstream.KeyValue

	// distributedSync is the singleton mappingLocker used to serialise
	// concurrent read-modify-write operations on shared mapping state.
	// Callers pass fully-qualified lock keys (including any namespace prefix).
	// TODO: When migrating handlers to the wrapper services, review the
	// initialization pattern — a global singleton may not fit the target
	// design (globals can be harder to test, maintain, and reason about),
	// so the lock backend, lifecycle, and injection strategy will need
	// a proper design review.
	distributedSync mappingLocker
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

	// Initialize the distributed sync singleton backed by the mappings KV bucket.
	distributedSync = newKVMappingLocker(mappingsKV,
		withLockerOptionMaxRetries(mappingLockRetryAttempts),
		withLockerOptionRetryInterval(mappingLockRetryInterval),
		withLockerOptionTimeout(mappingLockTimeout),
	)

	// Create or get the JetStream pull consumer for v1 objects KV bucket
	// This replaces the KV Watch() method to enable horizontal scaling
	consumerName := "v1-sync-helper-kv-consumer"
	streamName := "KV_v1-objects"

	consumer, err := jsContext.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "$KV.v1-objects.>",
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
		MaxAckPending: 1000,
		Description:   "durable/shared KV bucket watcher for v1-sync-helper pods",
	})
	if err != nil {
		logger.With(errKey, err, "consumer", consumerName, "stream", streamName).Error("error creating JetStream pull consumer")
		os.Exit(1)
	}

	// Start consuming KV updates using the JetStream consumer with error handling.
	kvConsumerCtx, err := consumer.Consume(kvMessageHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		logger.With(errKey, err).Error("KV consumer error encountered")
	}))
	if err != nil {
		logger.With(errKey, err, "consumer", consumerName).Error("error starting KV consumer")
		os.Exit(1)
	}
	defer kvConsumerCtx.Stop()

	// Subscribe to WAL-listener events from the wal_listener stream
	walStreamName := "wal_listener"
	walConsumerName := "v1-sync-helper-wal-consumer"

	// Create or get consumer for WAL listener events
	walConsumer, err := jsContext.CreateOrUpdateConsumer(ctx, walStreamName, jetstream.ConsumerConfig{
		Name:          walConsumerName,
		Durable:       walConsumerName,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "wal_listener.*",
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
		MaxAckPending: 100,
		Description:   "WAL listener consumer for v1-sync-helper",
	})
	if err != nil {
		logger.With(errKey, err, "consumer", walConsumerName, "stream", walStreamName).Error("error creating WAL listener consumer")
		os.Exit(1)
	}

	// Start consuming WAL listener messages with error handling.
	walConsumerCtx, err := walConsumer.Consume(walIngestHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		logger.With(errKey, err).Error("WAL consumer error encountered")
	}))
	if err != nil {
		logger.With(errKey, err, "consumer", walConsumerName).Error("error starting WAL listener consumer")
		os.Exit(1)
	}
	defer walConsumerCtx.Stop()

	// Optionally subscribe to DynamoDB stream events.
	var dynamodbConsumerCtx jetstream.ConsumeContext
	if cfg.DynamoDBIngestEnabled {
		dynamodbStreamName := cfg.DynamoDBStreamName
		dynamodbConsumerName := "v1-sync-helper-dynamodb-consumer"

		dynamodbConsumer, err := jsContext.CreateOrUpdateConsumer(ctx, dynamodbStreamName, jetstream.ConsumerConfig{
			Name:          dynamodbConsumerName,
			Durable:       dynamodbConsumerName,
			DeliverPolicy: jetstream.DeliverAllPolicy,
			AckPolicy:     jetstream.AckExplicitPolicy,
			FilterSubject: dynamodbStreamName + ".>",
			MaxDeliver:    3,
			AckWait:       30 * time.Second,
			MaxAckPending: 100,
			Description:   "DynamoDB stream consumer for v1-sync-helper",
		})
		if err != nil {
			logger.With(errKey, err, "consumer", dynamodbConsumerName, "stream", dynamodbStreamName).Error("error creating DynamoDB stream consumer")
			os.Exit(1)
		}

		dynamodbConsumerCtx, err = dynamodbConsumer.Consume(dynamodbIngestHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			logger.With(errKey, err).Error("DynamoDB stream consumer error encountered")
		}))
		if err != nil {
			logger.With(errKey, err, "consumer", dynamodbConsumerName).Error("error starting DynamoDB stream consumer")
			os.Exit(1)
		}
		defer dynamodbConsumerCtx.Stop()

		logger.With("stream", dynamodbStreamName, "consumer", dynamodbConsumerName).Info("DynamoDB stream consumer started")
	}

	// Subscribe to the lookup function for bidirectional v1-v2 mapping queries.
	// Supports both v1->v2 and v2->v1 lookups depending on the key format used.
	_, err = natsConn.QueueSubscribe(lookupSubject, natsQueue, lookupHandler)
	if err != nil {
		logger.With(errKey, err, "subject", lookupSubject).Error("error subscribing to NATS lookup subject")
		os.Exit(1)
	}

	// This next line blocks until SIGINT or SIGTERM is received, or NATS disconnects.
	<-done

	// Begin graceful shutdown process.
	logger.Debug("beginning graceful shutdown")

	// Drain consumers first (non-blocking) to mitigate "nats: connection closed"
	// errors in the ConsumeErrHandler.
	kvConsumerCtx.Drain()
	walConsumerCtx.Drain()
	if dynamodbConsumerCtx != nil {
		dynamodbConsumerCtx.Drain()
	}

	// Cancel the background context.
	cancel()

	// Drain the connection, which will drain all remaining subscriptions, then
	// close the connection when complete (including the consumer draining).
	if !natsConn.IsClosed() && !natsConn.IsDraining() {
		logger.Info("draining NATS connection")
		if err := natsConn.Drain(); err != nil {
			logger.With(errKey, err).Error("error draining NATS connection")
			os.Exit(1)
		}
	}

	// Wait for the graceful shutdown steps to complete.
	logger.Debug("waiting for graceful shutdown steps to complete")
	gracefulCloseWG.Wait()
	logger.Debug("graceful shutdown steps completed")

	// Immediately close the HTTP server after graceful shutdown has finished.
	if err = httpServer.Close(); err != nil {
		logger.With(errKey, err).Error("http listener error on close")
	}
}
