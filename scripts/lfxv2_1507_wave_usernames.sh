#!/bin/sh
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT
#
# LFXV2-1507: Per-wave affected-username extraction wrapper.
#
# Runs scripts/lfxv2_1507_affected_usernames.sql once for the requested
# onboarding wave (slug lists baked in from waves.md) and writes
# lfxv2_1507_wave<N>_usernames.csv in the current directory. Then runs
# lfxv2_1507_categorize.sh against resolved.csv (LFXV2-2662 flagged accounts)
# when that file is present.
#
# Usage:
#   scripts/lfxv2_1507_wave_usernames.sh <wave>   # wave: 0, 1, 2, ... 9, 11
#
# Requires: snowsql configured with rsa_key.p8 in the working directory.

set -eu

WAVE="${1:?usage: $0 <wave-number>}"

# Slug lists per waves.md. Wave 0 is AAIF (predates the wave schedule).
case "$WAVE" in
  0) SLUGS="agentic-ai-foundation" ;;
  1) SLUGS="sonicfund,lfeurope,openhpc,openids-foundation,interuss,opencontainers,cloud-foundry,x402-foundation,openpowerfoundation,real-time-linux,ACT,todogroup" ;;
  2) SLUGS="aswf,jupyter-foundation,project-jupyter,ojsf,open-mainframe-project,aomedia,ocudu-ecosystem-foundation,neonephos-foundation,egate" ;;
  3) SLUGS="cncf,cdf,openssf,finos,lf-decentralized-trust,openwalletfoundation,jdf3mf" ;;
  4) SLUGS="openchain,lfedge,lfn,lfenergy,dpdk" ;;
  5) SLUGS="risc-v-international,open-software-development-initiative-for-risc-v-ecosystem,chips,zep,cip,cti,pqca" ;;
  6) SLUGS="lf-ai-foundation,pytorch,ccc,presto,aether-fund,magma-fund" ;;
  7) SLUGS="soda-foundation,openapi,opensearch-foundation,react-foundation,margo" ;;
  8) SLUGS="o3de,gql,xen,ebpf,finops" ;;
  9) SLUGS="openinfra-foundation,yocto,cephfoundation,tla" ;;
  11) SLUGS="lfresearch,iovisor,jdf" ;;
  *) echo "error: unknown wave '$WAVE' (waves 0-9 or 11)" >&2; exit 1 ;;
esac

OUT="lfxv2_1507_wave${WAVE}_usernames.csv"
rm -f "$OUT"

snowsql --accountname JNMHVWD-XPB85243 --username DEV_ERIC \
  --warehouse VIEWER --rolename DATA_DEV --private-key-path rsa_key.p8 \
  -o friendly=false -o header=true -o timing=false \
  -o variable_substitution=true \
  -o output_format=csv -o output_file="$OUT" \
  -D SLUGS="$SLUGS" \
  -f "$(dirname "$0")/lfxv2_1507_affected_usernames.sql"

echo "wrote $OUT ($(($(wc -l < "$OUT") - 1)) usernames)"

# Categorize against the LFXV2-2662 flagged-account list when available.
if [ -f resolved.csv ]; then
  "$(dirname "$0")/lfxv2_1507_categorize.sh" "$OUT" resolved.csv \
    > "lfxv2_1507_wave${WAVE}_categorized.csv"
  echo "wrote lfxv2_1507_wave${WAVE}_categorized.csv"
  # Quick summary of the flag column (last field; can't use cut because
  # earlier CSV fields contain quoted commas).
  awk -F, '{print $NF}' "lfxv2_1507_wave${WAVE}_categorized.csv" | tail -n +2 | sort | uniq -c
else
  echo "resolved.csv not found; skipping LFXV2-2662 categorization" >&2
fi
