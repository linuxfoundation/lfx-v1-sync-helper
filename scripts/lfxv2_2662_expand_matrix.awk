# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

# LFXV2-2662: Expand matrixed_buckets.csv to one username per line.
#
# Input columns: "ALIGNMENT_STATUS","DRILLDOWN","USER_COUNT","SAMPLES"
# Output columns: status,drilldown,username (no count; spaces in usernames preserved).
#
# Usage: awk -f scripts/lfxv2_2662_expand_matrix.awk matrixed_buckets.csv

BEGIN { FS = "\",\"" }
NR > 1 {
    status = $1; sub(/^"/, "", status)
    drilldown = $2
    # Samples span fields 4..NF (usernames contain no quotes; commas split them).
    samples = $4
    for (i = 5; i <= NF; i++) samples = samples "," $(i)
    sub(/"[[:space:]]*$/, "", samples)
    n = split(samples, users, ",")
    for (i = 1; i <= n; i++) {
        u = users[i]
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", u)
        if (u != "") print status "," drilldown "," u
    }
}
