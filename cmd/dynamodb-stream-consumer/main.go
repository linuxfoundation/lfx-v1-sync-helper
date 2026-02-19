// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The dynamodb-stream-consumer service reads DynamoDB Streams and publishes
// each change record as a JSON message to a NATS JetStream stream. It tracks
// per-shard sequence positions in a NATS KV bucket so that it can resume from
// where it left off after a restart.
//
// Published subjects use the form: {NATS_SUBJECT_PREFIX}.{table_name}
// (dots in table names are replaced with underscores).
//
// Required environment variables:
//
//	DYNAMODB_TABLES  Comma-separated list of DynamoDB table names to consume.
//
// Optional environment variables (with defaults):
//
//	NATS_URL                    nats://nats:4222
//	NATS_STREAM_NAME            dynamodb_streams
//	NATS_SUBJECT_PREFIX         dynamodb_streams
//	CHECKPOINT_BUCKET           dynamodb-stream-checkpoints
//	AWS_REGION                  us-east-1
//	START_FROM_LATEST           false  (use TRIM_HORIZON for new shards)
//	POLL_INTERVAL_MS            1000
//	SHARD_REFRESH_INTERVAL_SEC  30
//	PORT                        8080
//	BIND                        *
//	DEBUG                       false
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

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	errKey                  = "error"
	gracefulShutdownSeconds = 25
)

var (
	logger   *slog.Logger
	cfg      *Config
	natsConn *nats.Conn
)

func main() {
	var err error
	cfg, err = LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	var debug = flag.Bool("d", false, "enable debug logging")
	var port = flag.String("p", cfg.Port, "health checks port")
	var bind = flag.String("bind", cfg.Bind, "interface to bind on")
	flag.Parse()

	logOptions := &slog.HandlerOptions{}
	if cfg.Debug || *debug {
		logOptions.Level = slog.LevelDebug
		logOptions.AddSource = true
	}
	logger = slog.New(slog.NewJSONHandler(os.Stdout, logOptions))
	slog.SetDefault(logger)

	// Health check server.
	http.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "OK\n")
	})
	http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if natsConn == nil || !natsConn.IsConnected() || natsConn.IsDraining() {
			http.Error(w, "NATS connection not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "OK\n")
	})

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
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.With(errKey, err).Error("http listener error")
			os.Exit(1)
		}
	}()

	gracefulCloseWG := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Connect to NATS.
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
				gracefulCloseWG.Done()
				return
			}
			logger.Error("NATS max-reconnects exhausted; connection closed")
			done <- os.Interrupt
			time.Sleep(5 * time.Second)
			os.Exit(1)
		}),
	)
	if err != nil {
		logger.With(errKey, err).Error("error creating NATS client")
		os.Exit(1)
	}

	jsCtx, err := jetstream.New(natsConn)
	if err != nil {
		logger.With(errKey, err).Error("error creating JetStream context")
		os.Exit(1)
	}

	// Create (or update) the NATS JetStream stream that receives DynamoDB events.
	_, err = jsCtx.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        cfg.NATSStreamName,
		Subjects:    []string{cfg.NATSSubjectPrefix + ".>"},
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      14 * 24 * time.Hour,
		Storage:     jetstream.FileStorage,
		Compression: jetstream.S2Compression,
		Description: "DynamoDB Streams change events",
	})
	if err != nil {
		logger.With(errKey, err, "stream", cfg.NATSStreamName).Error("error creating NATS stream")
		os.Exit(1)
	}

	// Create (or get) the KV bucket used to store per-shard sequence checkpoints.
	checkpointKV, err := jsCtx.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      cfg.CheckpointBucket,
		Description: "DynamoDB stream shard sequence checkpoints",
		Storage:     jetstream.FileStorage,
		History:     1,
	})
	if err != nil {
		logger.With(errKey, err, "bucket", cfg.CheckpointBucket).Error("error creating checkpoint KV bucket")
		os.Exit(1)
	}

	// Load AWS configuration from the environment / instance profile.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		logger.With(errKey, err).Error("error loading AWS config")
		os.Exit(1)
	}

	// If a role ARN is configured, assume it via STS for cross-account DynamoDB access.
	if cfg.AssumeRoleARN != "" {
		logger.With("role_arn", cfg.AssumeRoleARN).Info("assuming IAM role for DynamoDB access")
		stsClient := sts.NewFromConfig(awsCfg)
		awsCfg.Credentials = stscreds.NewAssumeRoleProvider(stsClient, cfg.AssumeRoleARN)
	}

	dynClient := dynamodb.NewFromConfig(awsCfg)
	streamsClient := dynamodbstreams.NewFromConfig(awsCfg)

	// Start one TableConsumer per configured table.
	var consumerWG sync.WaitGroup
	for _, tableName := range cfg.Tables {
		tableName := tableName
		consumer := &TableConsumer{
			tableName:     tableName,
			config:        cfg,
			dynClient:     dynClient,
			streamsClient: streamsClient,
			js:            jsCtx,
			checkpointKV:  checkpointKV,
			logger:        logger.With("table", tableName),
		}
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
				logger.With(errKey, err, "table", tableName).Error("table consumer error")
			}
		}()
	}

	// Block until SIGINT / SIGTERM.
	<-done
	logger.Debug("beginning graceful shutdown")

	// Cancel the context so all consumer goroutines exit.
	cancel()
	consumerWG.Wait()

	// Drain the NATS connection (flushes pending publishes).
	if !natsConn.IsClosed() && !natsConn.IsDraining() {
		logger.Info("draining NATS connection")
		if err := natsConn.Drain(); err != nil {
			logger.With(errKey, err).Error("error draining NATS connection")
			os.Exit(1)
		}
	}

	gracefulCloseWG.Wait()
	logger.Debug("graceful shutdown complete")

	if err = httpServer.Close(); err != nil {
		logger.With(errKey, err).Error("http listener error on close")
	}
}
