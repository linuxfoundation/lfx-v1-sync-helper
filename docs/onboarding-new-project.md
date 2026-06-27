# Onboarding a New Project

## 1. Add to Allow List

Update `cmd/lfx-v1-sync-helper/config.go`:

- **Single project only:** Add the project slug to `projectAllowlist`.
- **Project + all children:** Add the project slug to `projectFamilyAllowlist`.

## 2. Replay the Project Entry

The project entry has likely already been processed and skipped. To replay it, update the `v1-objects` NATS KV bucket with the same value it currently has:

```sh
# Get the current value
nats kv get v1-objects "salesforce-project__c.<sfid>"  > <sfid>

# Put it back (triggers reprocessing)
nats kv put v1-objects "salesforce-project__c.<sfid>" < <sfid>
# Remove temporary file
rm <sfid>
```

> **Note:** Deleting the WAL listener consumer also works but is not recommended.

## 3. Verify Mappings

Confirm both forward and reverse mappings exist in the `v1-mappings` bucket:

```sh
nats kv get v1-mappings "project.uid.<uid>"
nats kv get v1-mappings "project.sfid.<sfid>"
```

- `project.sfid.<sfid>` should contain the project UID.
- `project.uid.<uid>` should contain the SFID.

If either mapping is missing, create it:

```sh
nats kv create v1-mappings "project.sfid.<sfid>" "<uid>"
nats kv create v1-mappings "project.uid.<uid>" "<sfid>"
```

## 4. Reindex Committees

Committees must be indexed **before** other resources (meetings, mailing lists, etc.) since those depend on them.

- **Order matters:** parent committees must exist before subcommittees.
- Run the reindex multiple times if needed until all committees (and their members) are added.

## 5. Reindex Remaining Resources

Once committees and committee members are indexed, proceed with:

- Meetings
- Votes
- Surveys
- Mailing lists
- Any other resources

See [`scripts/reindexing/`](../scripts/reindexing/) for reindexing scripts.
