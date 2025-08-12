    #!/bin/bash

    set -o pipefail

    error_exit() {
      echo "ERROR: $1" >&2
      exit "${2:-1}"
    }

    s3_retry() {
      local attempt=1
      local max_attempts=3
      local delay_seconds=2
      while true; do
        "$@" && return 0
        if [[ $attempt -ge $max_attempts ]]; then
          return 1
        fi
        sleep $(( delay_seconds * attempt ))
        attempt=$(( attempt + 1 ))
      done
    }

    echo "Checking the Quay installation..."
    echo "-- Namespace: $QUAY_NAMESPACE"
    if ! kubectl get namespace "$QUAY_NAMESPACE" >/dev/null 2>&1; then
      error_exit "Namespace $QUAY_NAMESPACE does not exist or is not accessible" 200
    fi

    echo "-- Quay CRs..."
    QUAY_DEPLOYMENT=$(kubectl get deployment -n "$QUAY_NAMESPACE" -l quay-component=quay -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$QUAY_DEPLOYMENT" ]]; then
      error_exit "Failed to get Quay deployment name in namespace $QUAY_NAMESPACE" 206
    fi
    CLAIR_DEPLOYMENT=$(kubectl get deployment -n "$QUAY_NAMESPACE" -l quay-component=clair-app -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$CLAIR_DEPLOYMENT" ]]; then
      error_exit "Failed to get Clair deployment name in namespace $QUAY_NAMESPACE" 218
    fi
    MIRROR_DEPLOYMENT=$(kubectl get deployment -n "$QUAY_NAMESPACE" -l quay-component=quay-mirror -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$MIRROR_DEPLOYMENT" ]]; then
      error_exit "Failed to get Quay mirror deployment name in namespace $QUAY_NAMESPACE" 219
    fi
    OPERATOR_DEPLOYMENT=$(kubectl get deployment -n "$QUAY_NAMESPACE" -l olm.managed=true -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$OPERATOR_DEPLOYMENT" ]]; then
      error_exit "Failed to get Operator deployment name in namespace $QUAY_NAMESPACE" 220
    fi
    echo "Quay deployment: $QUAY_DEPLOYMENT"
    echo "Clair deployment: $CLAIR_DEPLOYMENT"
    echo "Mirror deployment: $MIRROR_DEPLOYMENT"
    echo "Operator deployment: $OPERATOR_DEPLOYMENT"

    echo "Checking S3 connectivity"
    if ! s3_retry aws s3 ls "s3://$OBJECT_BUCKET" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl >/dev/null 2>&1; then
      error_exit "Unable to connect to S3 (bucket: $OBJECT_BUCKET, endpoint: $S3_ENDPOINT). Verify endpoint, credentials, and network." 100
    fi
    echo "-- S3 connectivity is ok"

    echo "Sync the most recent backup..."
    echo "-- Find most recent backup in Cohesity"
    MOST_RECENT=$(s3_retry aws s3 ls "s3://$OBJECT_BUCKET/" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl | grep PRE | sort -nr | head -n 1 | awk '/PRE/ {print $2}' | sed 's:/$::')

    if [[ -z "$MOST_RECENT" ]]; then
      error_exit "No backups found in S3 bucket $OBJECT_BUCKET" 101
    fi
    echo "-- Most recent backup is $MOST_RECENT"

    RESTORE_DIR="$MOST_RECENT"
    echo "-- Create restore directory: $RESTORE_DIR"
    if ! mkdir -p "/backup/$RESTORE_DIR"; then
      error_exit "Failed to create restore directory /backup/$RESTORE_DIR" 102
    fi

    echo "-- List bucket contents"
    if ! s3_retry aws s3 ls "s3://$OBJECT_BUCKET/$MOST_RECENT" --recursive --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl ; then
      error_exit "Failed to list backup contents at s3://$OBJECT_BUCKET/$MOST_RECENT" 103
    fi

    echo "-- Sync backup files from S3 to restore directory"
    if ! s3_retry aws s3 sync "s3://$OBJECT_BUCKET/$MOST_RECENT" "/backup/$RESTORE_DIR" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl ; then
      error_exit "S3 sync failure from s3://$OBJECT_BUCKET/$MOST_RECENT to /backup/$RESTORE_DIR" 104
    fi

    echo "-- List backup files"
    ls -l "/backup/$RESTORE_DIR/."

    if [[ ! -s "/backup/$RESTORE_DIR/backup.sql" ]]; then
      error_exit "Database dump missing or empty at /backup/$RESTORE_DIR/backup.sql" 105
    fi

    echo "Prepare the restore..."
    # Extract registryEndpoint from QuayRegistry
    echo "-- Extracting registry endpoint from QuayRegistry"
    REGISTRY_NAME=$(kubectl get quayregistry -n "$QUAY_NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$REGISTRY_NAME" ]]; then
      error_exit "Failed to find a QuayRegistry in namespace $QUAY_NAMESPACE" 213
    fi
    echo "-- QuayRegistry name: $REGISTRY_NAME"
    REGISTRY_ENDPOINT=$(kubectl get quayregistry "$REGISTRY_NAME" -n "$QUAY_NAMESPACE" -o jsonpath='{.status.registryEndpoint}')
    if [[ -z "$REGISTRY_ENDPOINT" ]]; then
      error_exit "Failed to read .status.registryEndpoint from QuayRegistry/$REGISTRY_NAME" 214
    fi
    echo "-- registryEndpoint: $REGISTRY_ENDPOINT"
    REGISTRY_ENDPOINT_HOST="${REGISTRY_ENDPOINT#https://}"
    echo "-- registryEndpoint host: $REGISTRY_ENDPOINT_HOST"

    # Prepare managed-secret-keys patched copy
    SOURCE_KEYS_FILE="/backup/$RESTORE_DIR/managed-secret-keys.yaml"
    PATCHED_KEYS_FILE="/backup/$RESTORE_DIR/managed-secret-keys.patched.yaml"
    echo "-- Managed keys source: $SOURCE_KEYS_FILE"
    echo "-- Managed keys patched copy: $PATCHED_KEYS_FILE"
    if [[ ! -f "$SOURCE_KEYS_FILE" ]]; then
      error_exit "managed-secret-keys.yaml not found at $SOURCE_KEYS_FILE" 211
    fi
    # Rewrite quay-registry-hostname annotation to registryEndpoint host (no scheme)
    echo "-- Patch managed-secret-keys.yaml"
    if ! awk -v newval="$REGISTRY_ENDPOINT_HOST" '
      BEGIN { updated = 0 }
      {
        if ($0 ~ /^[[:space:]]*quay-registry-hostname:/) {
          # preserve leading indentation
          match($0, /[^[:space:]]/);
          indent = substr($0, 1, RSTART-1);
          print indent "quay-registry-hostname: " newval;
          updated = 1;
          next;
        }
        print $0;
      }
      END { if (!updated) exit 42 }
    ' "$SOURCE_KEYS_FILE" > "$PATCHED_KEYS_FILE"; then
      error_exit "Failed to write patched managed-secret-keys to $PATCHED_KEYS_FILE" 216
    fi
    # Verify update happened
    if ! grep -q "quay-registry-hostname: .*" "$PATCHED_KEYS_FILE"; then
      error_exit "Failed to update quay-registry-hostname in $PATCHED_KEYS_FILE" 217
    fi

    # If backup available, scale down Quay
    echo "Scale down Quay deployments..."
    
    OPERATOR_REPLICAS=$(kubectl get deployment "$OPERATOR_DEPLOYMENT" -n "$QUAY_NAMESPACE" -o jsonpath='{.spec.replicas}')
    echo "-- Scaling Quay operator down from $OPERATOR_REPLICAS replicas"
    if ! kubectl scale deployment "$OPERATOR_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas=0; then
      error_exit "Failed to scale down Quay operator deployment $OPERATOR_DEPLOYMENT in namespace $QUAY_NAMESPACE" 204
    fi

    QUAY_REPLICAS=$(kubectl get deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" -o jsonpath='{.spec.replicas}')
    echo "-- Scaling Quay app down from $QUAY_REPLICAS replicas"
    if ! kubectl scale deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas=0; then
      error_exit "Failed to scale down Quay deployment $QUAY_DEPLOYMENT in namespace $QUAY_NAMESPACE" 204
    fi

    CLAIR_REPLICAS=$(kubectl get deployment "$CLAIR_DEPLOYMENT" -n "$QUAY_NAMESPACE" -o jsonpath='{.spec.replicas}')
    echo "-- Scaling Clair app down from $CLAIR_REPLICAS replicas"
    if ! kubectl scale deployment "$CLAIR_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas=0; then
      error_exit "Failed to scale down Clair deployment $CLAIR_DEPLOYMENT in namespace $QUAY_NAMESPACE" 204
    fi

    MIRROR_REPLICAS=$(kubectl get deployment "$MIRROR_DEPLOYMENT" -n "$QUAY_NAMESPACE" -o jsonpath='{.spec.replicas}')
    echo "-- Scaling Quay mirror down from $MIRROR_REPLICAS replicas"
    if ! kubectl scale deployment "$MIRROR_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas=0; then
      error_exit "Failed to scale down Quay mirror deployment $MIRROR_DEPLOYMENT in namespace $QUAY_NAMESPACE" 204
    fi

    restore_replicas() {
      echo "Restoring Quay deployments..."
      if [[ -n "$QUAY_REPLICAS" ]]; then
        echo "-- Restoring Quay replicas to $QUAY_REPLICAS"
        kubectl scale deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas="$QUAY_REPLICAS" || true
      fi
      if [[ -n "$CLAIR_REPLICAS" ]]; then
        echo "-- Restoring Clair replicas to $CLAIR_REPLICAS"
        kubectl scale deployment "$CLAIR_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas="$CLAIR_REPLICAS" || true
      fi
      if [[ -n "$MIRROR_REPLICAS" ]]; then
        echo "-- Restoring Quay mirror replicas to $MIRROR_REPLICAS"
        kubectl scale deployment "$MIRROR_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas="$MIRROR_REPLICAS" || true
      fi    
      if [[ -n "$OPERATOR_REPLICAS" ]]; then
        echo "-- Restoring Quay operator replicas to $OPERATOR_REPLICAS"
        kubectl scale deployment "$OPERATOR_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas="$OPERATOR_REPLICAS" || true
      fi             
    }
    trap restore_replicas EXIT

    echo "Waiting for Quay deployment to scale down and pods to terminate"
    if kubectl get pods -n "$QUAY_NAMESPACE" -l quay-component=quay --no-headers 2>/dev/null | grep -q .; then
      if ! kubectl wait --for=delete pod -l quay-component=quay -n "$QUAY_NAMESPACE" --timeout=300s; then
        error_exit "Timed out waiting for Quay pods to terminate after scaling down" 203
      fi
    fi

    echo "Perform a clean Quay database restore"
    DB_POD_NAME=$(kubectl get pod -l quay-component=postgres -n "$QUAY_NAMESPACE" -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$DB_POD_NAME" ]]; then
      error_exit "Failed to get Postgres pod name in $QUAY_NAMESPACE namespace" 202
    fi
    if ! kubectl exec -i pod/"$DB_POD_NAME" -n "$QUAY_NAMESPACE" -- sh -c '/usr/bin/dropdb --if-exists -U $POSTGRESQL_USER $POSTGRESQL_DATABASE'; then
      error_exit "Failed to drop database $POSTGRESQL_DATABASE in pod $DB_POD_NAME" 207
    fi
    if ! kubectl exec -i pod/"$DB_POD_NAME" -n "$QUAY_NAMESPACE" -- sh -c '/usr/bin/createdb -U $POSTGRESQL_USER $POSTGRESQL_DATABASE'; then
      error_exit "Failed to create database $POSTGRESQL_DATABASE in pod $DB_POD_NAME" 208
    fi
    if ! kubectl exec -i pod/"$DB_POD_NAME" -n "$QUAY_NAMESPACE" -- sh -c '/usr/bin/psql -U $POSTGRESQL_USER $POSTGRESQL_DATABASE -f /backup/$RESTORE_DIR/backup.sql'; then
      error_exit "Failed to restore database from /backup/$RESTORE_DIR/backup.sql in pod $DB_POD_NAME" 209
    fi

     echo "Restoring Quay deployments to original replica counts"
     restore_replicas
     trap - EXIT

    echo "Delete local backup copies older than ${RETENTION_PERIOD} days"
    find /backup/* -type d -mtime +"${RETENTION_PERIOD}" -print -exec rm -rf {} +

    echo "Done"