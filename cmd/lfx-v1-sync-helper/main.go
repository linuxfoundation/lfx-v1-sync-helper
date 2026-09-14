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

	"github.com/jackc/pgx/v5/pgxpool"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	errKey            = "error"
	defaultListenPort = "8080"
	defaultListenBind = "*"
	natsQueue         = "lfx.v1-sync-helper.queue"
	lookupSubject     = "lfx.lookup_v1_mapping"
	// User SFID lookup subjects for resolving v1 platform user SFIDs by username or email.
	lookupUserSFIDByUsernameSubject = "lfx.lookup_v1_user_sfid.by_username"
	lookupUserSFIDByEmailSubject    = "lfx.lookup_v1_user_sfid.by_email"
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

	// mappingStore is the abstract v1-mappings backing store used by
	// every online (non-backfill) code path and by the one-shot
	// backfills that read/write mappings.
	//
	// Two-phase initialisation (see main below):
	//
	//   1. Immediately after the v1-mappings KV bucket handle is
	//      opened, mappingStore is wired to a KV-backed adapter
	//      (newKVMappingStore) so one-shot flags dispatched next in
	//      main can safely call mappingStore.Get/Put/etc. This is
	//      NOT gated on V1_MAPPINGS_STORE_MODE — every one-shot runs
	//      against the KV backend regardless of the runtime mode
	//      because Postgres is not part of the one-shot contract.
	//   2. If the process is on the long-running service path (no
	//      one-shot flag fired), initMappingStore reassigns
	//      mappingStore to the mode-configured backend (kv | dual |
	//      postgres). Dual mode wraps KV in dualMappingStore which
	//      spawns the async mirror worker; graceful shutdown
	//      type-asserts and calls Close() to drain pending mirrors.
	//
	// See mapping_store.go for the port and V1MappingsStoreMode
	// selection.
	mappingStore MappingStore

	// distributedSync is the singleton mappingLocker used to serialise
	// concurrent read-modify-write operations on shared mapping state.
	// Callers pass fully-qualified lock keys (including any namespace prefix).
	// TODO: When migrating handlers to the wrapper services, review the
	// initialization pattern — a global singleton may not fit the target
	// design (globals can be harder to test, maintain, and reason about),
	// so the lock backend, lifecycle, and injection strategy will need
	// a proper design review.
	distributedSync mappingLocker //nolint:unused
)

// main parses optional flags and starts the NATS subscribers.
func main() {
	var debug = flag.Bool("d", false, "enable debug logging")
	var port = flag.String("p", "", "health checks port")
	var bind = flag.String("bind", "", "interface to bind on")
	var doBackfillACSProject = flag.Bool("backfill-acs-project", false, "backfill ACS user grants to v2 project settings, then exit")
	var doBackfillACSOrg = flag.Bool("backfill-acs-org", false, "backfill ACS org grants to v2 b2b_org settings, then exit")
	var doBackfillAltEmails = flag.Bool("backfill-alternate-emails", false, "backfill v1 alternate emails to Auth0 linked identities, then exit")
	var doBackfillProfiles = flag.Bool("backfill-profiles", false, "backfill v1 profile fields to Auth0 user_metadata, then exit")
	var doBackfillWorkspaces = flag.Bool("backfill-workspaces", false, "backfill legacy workspaces into v2 member-service, then exit")
	var doBackfillCommitteeMemberMappings = flag.Bool("backfill-committee-member-mappings", false, "repair committee-member reverse mappings that store the record sfid instead of the contact SFID, then exit")
	var doBackfillCommitteeMemberNames = flag.Bool("backfill-committee-member-names", false, "populate first_name/last_name on V2 committee members that have no name (members without an LFX account at sync time), then exit")
	var doBackfillV1MappingsToPG = flag.Bool("backfill-v1-mappings-to-postgres", false, "copy the v1-mappings NATS KV bucket into the Postgres v1_mappings table, then exit (LFXV2-2985)")
	var syncUser = flag.String("sync-user", "", "sync profile and alternate emails for a single user by username, then exit")
	var dryRun = flag.Bool("dry-run", false, "log changes without writing them (applicable with --backfill-* and --sync-user)")
	var backfillLimit = flag.Int("limit", 1000, "maximum number of users to process per backfill run (applicable with --backfill-alternate-emails and --backfill-profiles)")

	flag.Usage = func() {
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()

	// Enforce mutual exclusion across all one-shot flags.
	oneShotCount := 0
	for _, b := range []bool{*doBackfillACSProject, *doBackfillACSOrg, *doBackfillWorkspaces, *doBackfillAltEmails, *doBackfillProfiles, *syncUser != "", *doBackfillCommitteeMemberMappings, *doBackfillCommitteeMemberNames, *doBackfillV1MappingsToPG} {
		if b {
			oneShotCount++
		}
	}
	if oneShotCount > 1 {
		fmt.Fprintln(os.Stderr, "error: --backfill-acs-project, --backfill-acs-org, --backfill-workspaces, --backfill-alternate-emails, --backfill-profiles, --backfill-committee-member-mappings, --backfill-committee-member-names, --backfill-v1-mappings-to-postgres, and --sync-user are mutually exclusive")
		os.Exit(2)
	}

	// Initialize a default logger early so init functions can log errors.
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
	slog.SetDefault(logger)

	// --backfill-committee-member-mappings and --backfill-v1-mappings-to-postgres
	// only need NATS KV (plus Postgres for the v1-mappings backfill); skip full
	// config and API client init.
	// --backfill-acs-project and --backfill-acs-org require full config and API client init.
	var err error
	if *doBackfillCommitteeMemberMappings || *doBackfillV1MappingsToPG {
		cfg = LoadMinimalConfig()
	} else {
		cfg, err = LoadConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
			os.Exit(1)
		}
		if err := initV1DB(context.Background(), cfg); err != nil {
			logger.With(errKey, err).Error("error initializing v1 platform database connection")
			os.Exit(1)
		}
		if err := initJWTClient(cfg); err != nil {
			logger.With(errKey, err).Error("error initializing JWT client")
			os.Exit(1)
		}
		if err := initGoaClients(cfg); err != nil {
			logger.With(errKey, err).Error("error initializing Goa clients")
			os.Exit(1)
		}
		if err := initV1Client(cfg); err != nil {
			logger.With(errKey, err).Error("error initializing v1 client")
			os.Exit(1)
		}
		if err := initAuth0MgmtClient(cfg); err != nil {
			logger.With(errKey, err).Error("error initializing Auth0 Management API client")
			os.Exit(1)
		}
	}

	// Reinitialize logger with debug options if requested.
	if cfg.Debug || *debug {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: true,
		}))
		slog.SetDefault(logger)
	}

	// Apply defaults for port/bind from config if not set via flags.
	if *port == "" {
		*port = cfg.Port
	}
	if *bind == "" {
		*bind = cfg.Bind
	}
	if *port == "" {
		*port = defaultListenPort
	}
	if *bind == "" {
		*bind = defaultListenBind
	}

	// Support GET/POST monitoring "ping".
	http.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		// This always returns as long as the service is still running. As this
		// endpoint is expected to be used as a Kubernetes liveness check, this
		// service must likewise self-detect non-recoverable errors and
		// self-terminate.
		fmt.Fprintf(w, "OK\n") //nolint:errcheck
	})

	// Basic health check.
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if natsConn == nil {
			http.Error(w, "no NATS connection", http.StatusServiceUnavailable)
			return
		}
		if !natsConn.IsConnected() || natsConn.IsDraining() {
			http.Error(w, "NATS connection not ready", http.StatusServiceUnavailable)
			return
		}
		// In postgres-only mode the pod cannot serve any mapping
		// read/write without a live Postgres connection, so a
		// dead-pool pod must fail readiness so the Service stops
		// routing to it. Dual mode is deliberately NATS-authoritative
		// (Postgres is a best-effort shadow) so a PG outage does NOT
		// affect readiness there; the diff-scan tooling catches the
		// resulting drift before cutover.
		if cfg != nil && cfg.V1MappingsStoreMode == V1MappingsStoreModePostgres && pgPool != nil {
			pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := pgPool.Ping(pingCtx); err != nil {
				http.Error(w, "Postgres not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		fmt.Fprintf(w, "OK\n") //nolint:errcheck
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

	// Support graceful shutdown. signal.NotifyContext wires SIGINT /
	// SIGTERM directly to the process-wide context — this way the
	// one-shot backfills (which run long-running scans and exit
	// before the normal `<-done>` service loop is reached) observe a
	// kubectl-driven cancellation and clean up on their own,
	// including the deferred staging-table DROP in
	// backfill_v1_mappings_pg.go. The long-running service path also
	// exits when ctx is cancelled via the shared done channel.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

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

	// Initialize a KV-backed mappingStore immediately so one-shot backfills
	// that read/write mappings do not hit a nil interface. The
	// mode-configured mappingStore for the long-running service path (which
	// may open a Postgres pool for dual/postgres modes) is set later, after
	// all one-shot branches have exited. This intentionally keeps every
	// one-shot on the KV backend regardless of V1_MAPPINGS_STORE_MODE:
	// Postgres is not part of the one-shot contract yet, and the migration
	// story assumes the KV bucket stays authoritative for the offline
	// backfill window (LFXV2-2985).
	mappingStore = newKVMappingStore(mappingsKV)

	// Handle --backfill-acs-project flag: populate v2 project settings from ACS grants, then exit.
	if *doBackfillACSProject {
		logger.With("dry_run", *dryRun).Info("starting ACS project grants backfill")
		if err := backfillACSProjectGrants(ctx, *dryRun); err != nil {
			logger.With(errKey, err).Error("error during ACS project grants backfill")
			os.Exit(1)
		}
		logger.Info("ACS project grants backfill completed successfully")
		os.Exit(0)
	}

	// Handle --backfill-acs-org flag: populate v2 b2b_org settings from ACS org grants, then exit.
	if *doBackfillACSOrg {
		logger.With("dry_run", *dryRun).Info("starting ACS org grants backfill")
		if err := backfillACSOrgGrants(ctx, *dryRun); err != nil {
			logger.With(errKey, err).Error("error during ACS org grants backfill")
			os.Exit(1)
		}
		logger.Info("ACS org grants backfill completed successfully")
		os.Exit(0)
	}

	// Handle --backfill-alternate-emails flag: link v1 verified alternate emails to Auth0, then exit.
	if *doBackfillAltEmails {
		logger.With("limit", *backfillLimit, "dry_run", *dryRun).Info("starting alternate-emails backfill")
		result, err := backfillAlternateEmails(ctx, *backfillLimit, *dryRun)
		if err != nil {
			logger.With(errKey, err).Error("error during alternate-emails backfill")
			os.Exit(1)
		}
		logger.With(
			"users_processed", result.usersProcessed,
			"emails_linked", result.emailsLinked,
			"emails_skipped", result.emailsSkipped,
		).Info("alternate-emails backfill completed successfully")
		os.Exit(0)
	}

	// Handle --backfill-profiles flag: sync v1 profile fields to Auth0 user_metadata, then exit.
	if *doBackfillProfiles {
		logger.With("limit", *backfillLimit, "dry_run", *dryRun).Info("starting profiles backfill")
		result, err := backfillProfiles(ctx, *backfillLimit, *dryRun)
		if err != nil {
			logger.With(errKey, err).Error("error during profiles backfill")
			os.Exit(1)
		}
		logger.With(
			"users_processed", result.usersProcessed,
			"users_updated", result.usersUpdated,
			"users_skipped", result.usersSkipped,
		).Info("profiles backfill completed successfully")
		os.Exit(0)
	}

	// Handle --sync-user flag: sync profile and alternate emails for a single user, then exit.
	if *syncUser != "" {
		logger.With("username", *syncUser, "dry_run", *dryRun).Info("starting single-user sync")
		if err := syncSingleUser(ctx, *syncUser, *dryRun); err != nil {
			logger.With(errKey, err).Error("error during single-user sync")
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Handle --backfill-workspaces flag: backfill legacy workspaces into v2 member-service, then exit.
	if *doBackfillWorkspaces {
		logger.With("dry_run", *dryRun).Info("starting workspace backfill")
		if err := backfillWorkspaces(ctx, *dryRun); err != nil {
			logger.With(errKey, err).Error("error during workspace backfill")
			os.Exit(1)
		}
		logger.Info("workspace backfill completed successfully")
		os.Exit(0)
	}

	// Handle --backfill-committee-member-mappings flag: repair committee-member
	// reverse mappings that store the record sfid instead of the contact SFID
	// (LFXV2-2673), then exit.
	if *doBackfillCommitteeMemberMappings {
		logger.With("dry_run", *dryRun).Info("starting committee-member reverse-mapping backfill")
		res, err := backfillCommitteeMemberMappings(ctx, *dryRun)
		if err != nil {
			logger.With(errKey, err).Error("error during committee-member reverse-mapping backfill")
			os.Exit(1)
		}
		logger.With(
			"inspected", res.inspected,
			"poisoned", res.poisoned,
			"fixed", res.fixed,
			"already_ok", res.alreadyOK,
			"unresolved", res.unresolved,
			"malformed", res.malformed,
			"tombstoned", res.tombstoned,
			"conflicted", res.conflicted,
		).Info("committee-member reverse-mapping backfill completed successfully")
		os.Exit(0)
	}

	// Handle --backfill-committee-member-names flag: populate first_name/last_name
	// on V2 committee members whose name fields are empty because the member had no
	// LFX account at sync time (linuxfoundation/lfx-self-serve-ops#3), then exit.
	if *doBackfillCommitteeMemberNames {
		logger.With("dry_run", *dryRun).Info("starting committee-member name backfill")
		res, err := backfillCommitteeMemberNames(ctx, *dryRun)
		if err != nil {
			logger.With(errKey, err).Error("error during committee-member name backfill")
			os.Exit(1)
		}
		logger.With(
			"inspected", res.inspected,
			"skipped", res.skipped,
			"no_mapping", res.noMapping,
			"no_name", res.noName,
			"updated", res.updated,
			"dry_run", res.dryRun,
			"errored", res.errored,
		).Info("committee-member name backfill completed successfully")
		os.Exit(0)
	}

	// Handle --backfill-v1-mappings-to-postgres flag: copy the v1-mappings NATS
	// KV bucket into the Postgres v1_mappings table, then exit (LFXV2-2985).
	if *doBackfillV1MappingsToPG {
		logger.With("dry_run", *dryRun).Info("starting v1-mappings Postgres backfill")
		// --dry-run needs Postgres credentials only when it will
		// actually connect; the backfill itself no-ops COPY/upsert
		// in dry-run mode, so open a pool WITHOUT running schema DDL
		// (that would fail on a read-only replica or without a
		// working DSN, which is exactly the case a dry-run should
		// tolerate). Non-dry-run uses initPGPoolWithSchema so the
		// v1_mappings table is created before the first COPY.
		var pool *pgxpool.Pool
		if *dryRun {
			pool, err = initPGPool(ctx, cfg)
		} else {
			pool, err = initPGPoolWithSchema(ctx, cfg)
		}
		if err != nil {
			logger.With(errKey, err).Error("error initializing Postgres pool for v1-mappings backfill")
			os.Exit(1)
		}
		defer pool.Close()
		res, err := backfillV1MappingsToPostgres(ctx, pool, *dryRun)
		fields := []any{
			"visits", res.visits,
			"live", res.live,
			"tombstoned", res.tombstoned,
			"empty", res.empty,
			"native_del", res.nativeDel,
			"staged", res.staged,
			"inserted_rows", res.insertedRows,
			"batches", res.batches,
			"workers", res.workers,
			"max_seq", res.maxSeq,
			"elapsed", res.elapsed.String(),
			"dry_run", res.dryRun,
		}
		if err != nil {
			logger.With(append(fields, errKey, err)...).Error("error during v1-mappings Postgres backfill")
			os.Exit(1)
		}
		logger.With(fields...).Info("v1-mappings Postgres backfill completed successfully")
		os.Exit(0)
	}

	// Initialize the distributed sync singleton backed by the mappings KV bucket.
	distributedSync = newKVMappingLocker(mappingsKV,
		withLockerOptionMaxRetries(mappingLockRetryAttempts),
		withLockerOptionRetryInterval(mappingLockRetryInterval),
		withLockerOptionTimeout(mappingLockTimeout),
	)

	// Wire the online MappingStore based on V1_MAPPINGS_STORE_MODE.
	// This is the runtime port that all non-backfill callers use to
	// read/write v1-mappings state. In "kv" mode the store is a thin
	// adapter over mappingsKV so behaviour is unchanged; in "dual" or
	// "postgres" mode a pgxpool is opened and the embedded schema is
	// applied idempotently before the store is exposed. See
	// mapping_store.go for the interface and semantic contract.
	mappingStore, err = initMappingStore(ctx, cfg, mappingsKV)
	if err != nil {
		logger.With(errKey, err, "mode", string(cfg.V1MappingsStoreMode)).Error("error initializing v1-mappings store")
		os.Exit(1)
	}
	logger.With("mode", string(cfg.V1MappingsStoreMode)).Info("v1-mappings store initialized")

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

	// Subscribe to user SFID lookup functions for resolving v1 platform user SFIDs.
	// These use secondary indexes with validation to handle stale data.
	_, err = natsConn.QueueSubscribe(lookupUserSFIDByUsernameSubject, natsQueue, userSFIDByUsernameHandler)
	if err != nil {
		logger.With(errKey, err, "subject", lookupUserSFIDByUsernameSubject).Error("error subscribing to user SFID by username lookup subject")
		os.Exit(1)
	}
	_, err = natsConn.QueueSubscribe(lookupUserSFIDByEmailSubject, natsQueue, userSFIDByEmailHandler)
	if err != nil {
		logger.With(errKey, err, "subject", lookupUserSFIDByEmailSubject).Error("error subscribing to user SFID by email lookup subject")
		os.Exit(1)
	}

	// Subscribe to auth-service profile update events for v2-to-v1 sync.
	_, err = natsConn.QueueSubscribe("lfx.user_profile.updated", natsQueue, handleUserProfileUpdated)
	if err != nil {
		logger.With(errKey, err, "subject", "lfx.user_profile.updated").Error("error subscribing to user profile updated subject")
		os.Exit(1)
	}

	// Subscribe to indexer domain events for bidirectional committee sync via a durable
	// JetStream consumer on the committee-events stream. The stream captures
	// lfx.committee.> and lfx.committee_member.> subjects published by the indexer service
	// after every successful OpenSearch write. Using JetStream gives at-least-once delivery,
	// replacing the at-most-once core NATS QueueSubscribe that could silently drop events.
	committeeEventsStreamName := "committee_events"
	committeeEventsConsumerName := "v1-sync-helper-committee-events-consumer"

	committeeEventsConsumer, err := jsContext.CreateOrUpdateConsumer(ctx, committeeEventsStreamName, jetstream.ConsumerConfig{
		Name:    committeeEventsConsumerName,
		Durable: committeeEventsConsumerName,
		// DeliverNewPolicy: only deliver messages published after this consumer is first created.
		// The old QueueSubscribe only processed events after subscription; replaying stream history
		// on first start would flood V1 with redundant updates. The durable name ensures the
		// consumer resumes from its last ACKed position on restarts, so no events are dropped
		// after the initial connection.
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubjects: []string{
			"lfx.committee.>",
			"lfx.committee_member.>",
		},
		MaxDeliver:    3,
		AckWait:       30 * time.Second,
		MaxAckPending: 100,
		Description:   "Indexer committee/committee-member event consumer for v1-sync-helper",
	})
	if err != nil {
		logger.With(errKey, err, "consumer", committeeEventsConsumerName, "stream", committeeEventsStreamName).Error("error creating committee-events consumer")
		os.Exit(1)
	}

	committeeEventsConsumerCtx, err := committeeEventsConsumer.Consume(committeeEventsIngestHandler, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		logger.With(errKey, err).Error("committee-events consumer error encountered")
	}))
	if err != nil {
		logger.With(errKey, err, "consumer", committeeEventsConsumerName).Error("error starting committee-events consumer")
		os.Exit(1)
	}
	defer committeeEventsConsumerCtx.Stop()

	logger.With("stream", committeeEventsStreamName, "consumer", committeeEventsConsumerName).Info("committee-events consumer started")

	// Subscribe to project indexer events via core NATS for bidirectional project sync.
	// The indexer publishes lfx.project.{action} after every successful OpenSearch write.
	for _, subject := range []string{"lfx.project.created", "lfx.project.updated", "lfx.project.deleted"} {
		if _, err = natsConn.QueueSubscribe(subject, natsQueue, projectIndexerEventHandler); err != nil {
			logger.With(errKey, err, "subject", subject).Error("error subscribing to project indexer event subject")
			os.Exit(1)
		}
	}

	// This next line blocks until SIGINT or SIGTERM is received, or NATS disconnects.
	<-done

	// Begin graceful shutdown process.
	logger.Debug("beginning graceful shutdown")

	// Drain consumers first (non-blocking) to mitigate "nats: connection closed"
	// errors in the ConsumeErrHandler.
	kvConsumerCtx.Drain()
	walConsumerCtx.Drain()
	committeeEventsConsumerCtx.Drain()
	if dynamodbConsumerCtx != nil {
		dynamodbConsumerCtx.Drain()
	}

	// Cancel the background context. signal.NotifyContext also cancels
	// ctx on SIGTERM, so this defensive cancel is idempotent when the
	// shutdown was signal-driven; it is required when the shutdown was
	// triggered by the NATS ClosedHandler synthesising an interrupt.
	stop()

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

	// Drain the online MappingStore's async mirror queue before we
	// close the Postgres pool. Only dualMappingStore has a background
	// worker; kv- and postgres-only modes are no-ops here (they don't
	// implement Closer).
	if closer, ok := mappingStore.(interface{ Close() error }); ok {
		logger.Debug("draining dual-store mirror queue")
		if err := closer.Close(); err != nil {
			logger.With(errKey, err).Warn("error draining mapping store on shutdown")
		}
	}

	// Close the Postgres pool if the online MappingStore backend
	// opened one (dual or postgres mode). Kv-only mode never allocates
	// pgPool so this is a no-op.
	if pgPool != nil {
		logger.Debug("closing Postgres pool")
		pgPool.Close()
	}

	// Immediately close the HTTP server after graceful shutdown has finished.
	if err = httpServer.Close(); err != nil {
		logger.With(errKey, err).Error("http listener error on close")
	}
}
