// Reindexes all v2 projects by fetching their UIDs from the projects KV bucket,
// resolving each to a Salesforce ID via v1-mappings, then re-putting the
// salesforce-project__c.<sfid> entry in v1-objects. The re-put triggers the
// sync-helper event handler as if the object had just arrived from Meltano.
//
// By default the script runs in dry-run mode and only prints the keys it would
// reindex. Pass --reindex to perform the actual re-puts.
//
// Usage:
//
//	go run ./scripts/reindex-projects [--reindex] [--uid <v2-project-uid>]
//
// Env:
//
//	NATS_URL - NATS server URL (default: nats://localhost:4222)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	reindex := flag.Bool("reindex", false, "re-put each key to trigger reindexing (default: dry run)")
	uid := flag.String("uid", "", "reindex a single project by v2 UID")
	flag.Parse()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	if !*reindex {
		fmt.Fprintln(os.Stderr, "dry-run mode — pass --reindex to actually re-put keys")
	}

	nc, err := natsio.Connect(natsURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create JetStream: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	projectsKV, err := js.KeyValue(ctx, "projects")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open projects bucket: %v\n", err)
		os.Exit(1)
	}

	mappingsKV, err := js.KeyValue(ctx, "v1-mappings")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open v1-mappings bucket: %v\n", err)
		os.Exit(1)
	}

	objectsKV, err := js.KeyValue(ctx, "v1-objects")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open v1-objects bucket: %v\n", err)
		os.Exit(1)
	}

	if *uid != "" {
		sfid, err := resolveSFID(ctx, mappingsKV, *uid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve v1 ID for uid %s: %v\n", *uid, err)
			os.Exit(1)
		}
		reindexOne(ctx, objectsKV, *uid, sfid, *reindex)
		return
	}

	reindexAll(ctx, projectsKV, mappingsKV, objectsKV, *reindex)
}

func reindexAll(ctx context.Context, projectsKV, mappingsKV, objectsKV jetstream.KeyValue, perform bool) {
	lister, err := projectsKV.ListKeys(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to list projects: %v\n", err)
		os.Exit(1)
	}

	total, reindexed, failed := 0, 0, 0
	start := time.Now()

	for key := range lister.Keys() {
		// Skip slug/ lookup keys — only process UUID keys.
		if strings.HasPrefix(key, "slug/") {
			continue
		}
		total++

		sfid, err := resolveSFID(ctx, mappingsKV, key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%d] SKIP %s: no v1 mapping: %v\n", total, key, err)
			failed++
			continue
		}

		if !perform {
			fmt.Printf("salesforce-project__c.%s  (uid: %s)\n", sfid, key)
			continue
		}

		if ok := put(ctx, objectsKV, key, sfid, reindexed+1, start); ok {
			reindexed++
		} else {
			failed++
		}
	}

	if perform {
		fmt.Fprintf(os.Stderr, "done — total: %d, reindexed: %d, failed: %d, elapsed: %s\n",
			total, reindexed, failed, time.Since(start).Round(time.Second))
	} else {
		fmt.Fprintf(os.Stderr, "total: %d\n", total)
	}

	if failed > 0 {
		os.Exit(1)
	}
}

func reindexOne(ctx context.Context, objectsKV jetstream.KeyValue, uid, sfid string, perform bool) {
	if !perform {
		fmt.Printf("salesforce-project__c.%s  (uid: %s)\n", sfid, uid)
		fmt.Fprintln(os.Stderr, "dry-run: pass --reindex to actually re-put")
		return
	}
	if !put(ctx, objectsKV, uid, sfid, 1, time.Now()) {
		os.Exit(1)
	}
}

// resolveSFID looks up the Salesforce ID for a v2 project UID via v1-mappings.
func resolveSFID(ctx context.Context, mappingsKV jetstream.KeyValue, uid string) (string, error) {
	entry, err := mappingsKV.Get(ctx, "project.uid."+uid)
	if err != nil {
		return "", err
	}
	sfid := strings.TrimSpace(string(entry.Value()))
	if sfid == "" || sfid == "!del" {
		return "", fmt.Errorf("mapping tombstoned or empty")
	}
	return sfid, nil
}

func put(ctx context.Context, objectsKV jetstream.KeyValue, uid, sfid string, n int, start time.Time) bool {
	key := "salesforce-project__c." + sfid

	entry, err := objectsKV.Get(ctx, key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%d] ERROR get %s (uid: %s): %v\n", n, key, uid, err)
		return false
	}

	if _, err := objectsKV.Put(ctx, key, entry.Value()); err != nil {
		fmt.Fprintf(os.Stderr, "[%d] ERROR put %s (uid: %s): %v\n", n, key, uid, err)
		return false
	}

	fmt.Fprintf(os.Stderr, "[%d] reindexed %s (uid: %s, elapsed: %s)\n", n, key, uid, time.Since(start).Round(time.Second))
	return true
}
