#!/usr/bin/env bash
# Looks up the V1 SFID for each V2 project UID listed in a UIDs file
# using the NATS JetStream KV bucket "v1-mappings".
#
# Usage:
#   NATS_URL=nats://... ./scripts/validate-v1-v2/fetch-project-v1-ids.sh [uids-file]
#
# Output is a "v2uid=v1sfid" file ready to pass to --projects-file.
# Defaults to project-uids.txt in the same directory if no file is given.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UIDS_FILE="${1:-$SCRIPT_DIR/project-uids.txt}"

if [[ ! -f "$UIDS_FILE" ]]; then
  echo "error: $UIDS_FILE not found" >&2
  exit 1
fi

while IFS= read -r uid; do
  [[ -z "$uid" ]] && continue
  v1=$(nats kv get v1-mappings "project.uid.$uid" --raw 2>/dev/null || true)
  if [[ -z "$v1" ]]; then
    echo "# WARNING: no V1 mapping found for $uid"
    continue
  fi
  echo "$uid=$v1"
done < "$UIDS_FILE"
