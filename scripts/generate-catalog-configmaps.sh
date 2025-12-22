#!/bin/bash
# Copyright The Linux Foundation and each contributor to LFX.
# SPDX-License-Identifier: MIT

set -euo pipefail

# Get the directory where this script is located.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Set Meltano environment variables to allow running from any directory.
MELTANO_PROJECT_ROOT="${MELTANO_PROJECT_ROOT:-${REPO_ROOT}/meltano}"
export MELTANO_PROJECT_ROOT
MELTANO_ENVIRONMENT=${MELTANO_ENVIRONMENT-dev}
export MELTANO_ENVIRONMENT

# Default values.
OUTPUT_FILE=""
TAPS=()

# Usage function.
usage() {
	cat <<EOF
Usage: $0 [OPTIONS] TAP_NAME...

Generate Kubernetes ConfigMaps from Meltano tap catalogs.

OPTIONS:
    -e, --environment ENV    Meltano environment to use (default: $MELTANO_ENVIRONMENT)
    -o, --output FILE        Output file path (default: stdout)
    -h, --help               Show this help message

TAP_NAME:
    One or more tap names to generate catalogs for:
    - tap-dynamodb
    - tap-postgres

EXAMPLES:
    # Generate ConfigMaps for both taps
    $0 tap-dynamodb tap-postgres

    # Use production environment
    $0 -e prod tap-dynamodb

    # Save to file
    $0 -o catalogs.yaml tap-dynamodb tap-postgres

    # Quick generation for all known taps
    $0 tap-dynamodb tap-postgres > meltano-catalogs.yaml

EOF
}

# Parse command line arguments.
while [[ $# -gt 0 ]]; do
	case $1 in
	-e | --environment)
		MELTANO_ENVIRONMENT="$2"
		export MELTANO_ENVIRONMENT
		shift 2
		;;
	-o | --output)
		OUTPUT_FILE="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	-*)
		echo "Error: Unknown option: $1" >&2
		usage >&2
		exit 1
		;;
	*)
		TAPS+=("$1")
		shift
		;;
	esac
done

# Validate input.
if [[ ${#TAPS[@]} -eq 0 ]]; then
	echo "Error: At least one tap name must be specified." >&2
	usage >&2
	exit 1
fi

# Verify we're in the correct location.
if [[ ! -f "${MELTANO_PROJECT_ROOT}/meltano.yml" ]]; then
	echo "Error: ${MELTANO_PROJECT_ROOT}/meltano.yml not found." >&2
	exit 1
fi

# Display current environment information.
echo "Meltano Environment: ${MELTANO_ENVIRONMENT}" >&2
echo "Meltano Project Root: ${MELTANO_PROJECT_ROOT}" >&2

# Check AWS access and display current account.
if aws_account=$(aws sts get-caller-identity --output text --query Account 2>/dev/null); then
	aws_info="${aws_account}"
	if [[ -n "${AWS_VAULT:-}" ]]; then
		aws_info="${aws_info} (${AWS_VAULT})"
	elif [[ -n "${AWS_PROFILE:-}" ]]; then
		aws_info="${aws_info} (${AWS_PROFILE})"
	fi
	echo "AWS Account: ${aws_info}" >&2
else
	echo "Warning: Unable to determine AWS account (aws sts get-caller-identity failed)" >&2
fi

echo "Waiting 5 seconds before continuing..." >&2
sleep 5

# Function to run meltano catalog dump for a specific tap.
run_meltano_catalog() {
	local tap_name="$1"
	local stderr_file="$2"

	echo "Generating catalog for ${tap_name}..." >&2

	# With MELTANO_PROJECT_ROOT and MELTANO_ENVIRONMENT set, we can run from anywhere.
	uv run meltano invoke --dump=catalog "${tap_name}" 2>"${stderr_file}"
}

# Function to generate complete ConfigMap YAML for a tap.
generate_configmap_yaml() {
	local tap_name="$1"
	local catalog_json="$2"

	# Sanitize tap name for use as ConfigMap name.
	local configmap_name="${tap_name//_/-}"
	configmap_name=$(echo "${configmap_name}" | tr '[:upper:]' '[:lower:]')

	cat <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${configmap_name}-catalog
  labels:
    app.kubernetes.io/name: lfx-v1-sync-helper
    app.kubernetes.io/component: meltano
    lfx.linuxfoundation.org/tap: ${tap_name}
data:
  catalog.json: |
    ${catalog_json}
EOF
}

# Main processing.
echo "Generating catalog ConfigMaps..." >&2
echo "Environment: ${MELTANO_ENVIRONMENT}" >&2
echo "Taps: ${TAPS[*]}" >&2

if [[ -n "${OUTPUT_FILE}" ]]; then
	echo "Output: ${OUTPUT_FILE}" >&2
fi

CONFIGMAPS=()

for tap_name in "${TAPS[@]}"; do
	# Create temporary file for stderr capture.
	stderr_file=$(mktemp)

	# Run meltano catalog command and capture output.
	if ! catalog_json=$(run_meltano_catalog "${tap_name}" "${stderr_file}"); then
		echo "Error: Failed to generate catalog for ${tap_name}" >&2
		echo "Meltano stderr output:" >&2
		cat "${stderr_file}" >&2
		rm -f "${stderr_file}"
		exit 1
	fi

	# Clean up stderr file on success.
	rm -f "${stderr_file}"

	# Validate JSON output.
	if ! echo "$catalog_json" | jq . >/dev/null 2>&1; then
		echo "Error: Invalid JSON output from ${tap_name} catalog" >&2
		exit 1
	fi

	# Filter out unselected streams and compact the JSON to save space.
	catalog_json=$(echo "$catalog_json" | jq -c '.streams |= map(select(.selected == true))')

	# Generate ConfigMap YAML.
	configmap_yaml=$(generate_configmap_yaml "${tap_name}" "${catalog_json}")
	CONFIGMAPS+=("${configmap_yaml}")
done

# Combine all ConfigMaps with YAML document separators.
output_content=""
for i in "${!CONFIGMAPS[@]}"; do
	if [[ $i -gt 0 ]]; then
		output_content="${output_content}
---
"
	fi
	output_content="${output_content}${CONFIGMAPS[$i]}"
done

# Output result.
if [[ -n "${OUTPUT_FILE}" ]]; then
	echo "$output_content" >"${OUTPUT_FILE}"
	echo "ConfigMaps written to ${OUTPUT_FILE}" >&2
else
	echo "$output_content"
fi
