# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Prepend the matrix bucket (alignment status + drilldown) to a
# resolved CSV, so the resolver output doubles as a manual-remediation
# reference.
#
# Inputs (in order):
#   1. matrixed_buckets.csv - output of lfxv2_2662_matrixed_buckets.sql
#                             ("STATUS","DRILLDOWN","COUNT","SAMPLES,...")
#   2. resolved.csv         - output of lfxv2_2662_resolve_usernames.sql
#                             (15 quoted columns, username first)
#
# Output: resolved.csv with two leading columns, "ALIGNMENT_STATUS" and
# "DRILLDOWN". Usernames absent from the matrix get empty bucket columns.
# Matching is case-insensitive on the username.
#
# Note: the apply scripts read columns positionally from column 1, so feed
# them the plain resolved.csv. This output is a reference/triage artifact.
#
# Usage:
#   awk -f scripts/lfxv2_2662_join_buckets.awk \
#       matrixed_buckets.csv resolved.csv > resolved_with_buckets.csv

BEGIN { FS = "\",\"" }

# Pass 1: matrixed_buckets.csv. Samples span fields 4..NF (usernames contain
# no quotes, so the shared comma splits them); spaces in usernames are kept.
NR == FNR {
    if (FNR == 1) next
    status = $1; sub(/^"/, "", status)
    drilldown = $2
    samples = $4
    for (i = 5; i <= NF; i++) samples = samples "," $(i)
    sub(/"[[:space:]]*$/, "", samples)
    n = split(samples, users, ",")
    for (i = 1; i <= n; i++) {
        u = users[i]
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", u)
        if (u != "") {
            k = tolower(u)
            bucket_status[k] = status
            bucket_drill[k] = drilldown
        }
    }
    next
}

# Pass 2: resolved.csv.
FNR == 1 { print "\"ALIGNMENT_STATUS\",\"DRILLDOWN\"," $0; next }
{
    u = $0
    sub(/^"/, "", u)
    sub(/".*$/, "", u)
    k = tolower(u)
    printf "\"%s\",\"%s\",%s\n", bucket_status[k], bucket_drill[k], $0
}
