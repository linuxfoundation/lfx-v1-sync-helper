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
#   scripts/lfxv2_1507_wave_usernames.sh <wave>   # wave: 0-11, or 4b, 5b, 7b
#
# Requires: snowsql configured with rsa_key.p8 in the working directory.

set -eu

WAVE="${1:?usage: $0 <wave-number>}"

# Slug lists for waves 0 to 7 follow waves.md; wave 0 is AAIF, which predates
# the wave schedule. Waves 7b and 8 onward follow the Manage Access Audit.
case "$WAVE" in
  0) SLUGS="agentic-ai-foundation" ;;
  1) SLUGS="sonicfund,lfeurope,openhpc,openids-foundation,interuss,opencontainers,cloud-foundry,x402-foundation,openpowerfoundation,real-time-linux,ACT,todogroup" ;;
  2) SLUGS="aswf,jupyter-foundation,project-jupyter,ojsf,open-mainframe-project,aomedia,ocudu-ecosystem-foundation,neonephos-foundation,egate" ;;
  3) SLUGS="cncf,cdf,openssf,finos,lf-decentralized-trust,openwalletfoundation,jdf3mf" ;;
  4) SLUGS="openchain,lfedge,lfn,lfenergy,dpdk" ;;
  # Wave 4b: Camara Fund belongs to wave 4 but was omitted from the wave 4
  # slug list, so it is picked up as a follow-on. camarafund is the fund
  # entity; the spine pulls in the Camara Project (telcoapi) beneath it.
  4b) SLUGS="camarafund" ;;
  5) SLUGS="risc-v-international,open-software-development-initiative-for-risc-v-ecosystem,chips,zep,cip,cti,pqca" ;;
  # Wave 5b: AGL was added to wave 5 after wave 5 had already run.
  5b) SLUGS="agl" ;;
  6) SLUGS="lf-ai-foundation,pytorch,ccc,presto,aether-fund,magma-fund" ;;
  7) SLUGS="soda-foundation,openapi,opensearch-foundation,react-foundation,margo" ;;
  # Wave 7b: HPSF appears only in the Manage Access Audit, which places it in
  # wave 7. It is in neither waves.md nor waves-expanded-with-slugs.md, so it
  # was never available to the earlier wave lists.
  7b) SLUGS="hpsf" ;;
  # Waves 8 to 11 follow the Manage Access Audit rather than waves.md, which
  # is stale for the back half of the schedule. The audit shifts xen, ebpf and
  # finops from wave 8 to wave 9, and places the already-run wave 7
  # foundations in its wave 8; those are omitted here as they are complete.
  8) SLUGS="o3de,gql,broadband-fund,daos-fund,murmur-project,p4-fund,socbb,tars" ;;
  9) SLUGS="openinfra-foundation,yocto,cephfoundation,tla,xen,ebpf,finops,device-automation-bus-fund,dronecode,kernelci,lf-charities,netdev-foundation,opifund" ;;
  10) SLUGS="akrites,aousd,app-defense-alliance,appia,c2pa,configurator-file-type-project,elisa,financial-services-open-source-ai-fund,green-software,lfai-onnx,overture,spaceone" ;;
  11) SLUGS="lfresearch,iovisor,jdf,alpha-omega-foundation,alphaomega,lfcf,oneapi,openchami,operator-sdk,quantum-ir,r-hub,rcons,spdx,uepf" ;;
  *) echo "error: unknown wave '$WAVE' (waves 0-11, 4b, 5b, or 7b)" >&2; exit 1 ;;
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
  echo ""
  echo "next:"
  echo "  scripts/lfxv2_1507_deploy_wave.sh $WAVE"
else
  echo "resolved.csv not found; skipping LFXV2-2662 categorization" >&2
fi
