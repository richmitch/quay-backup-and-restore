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

    echo "Checking namespace: $QUAY_NAMESPACE"
    if ! kubectl get namespace "$QUAY_NAMESPACE" >/dev/null 2>&1; then
      error_exit "Namespace $QUAY_NAMESPACE does not exist or is not accessible" 200
    fi

    echo "Checking S3 connectivity"
    if ! s3_retry aws s3 ls "s3://$OBJECT_BUCKET" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl >/dev/null 2>&1; then
      error_exit "Unable to connect to S3 (bucket: $OBJECT_BUCKET, endpoint: $S3_ENDPOINT). Verify endpoint, credentials, and network." 100
    fi

    echo "Find most recent backup in Cohesity"
    MOST_RECENT=$(s3_retry aws s3 ls "s3://$OBJECT_BUCKET" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl | grep PRE | sort -nr | head -n 1 | awk '/PRE/ {print $2}' | sed 's:/$::')

    if [[ -z "$MOST_RECENT" ]]; then
      error_exit "No backups found in S3 bucket $OBJECT_BUCKET" 101
    fi

    echo "Most recent backup is $MOST_RECENT"

    echo "Create restore directory"
    RESTORE_DIR="$MOST_RECENT"
    if ! mkdir -p "/backup/$RESTORE_DIR"; then
      error_exit "Failed to create restore directory /backup/$RESTORE_DIR" 102
    fi

    echo "List bucket contents"
    if ! s3_retry aws s3 ls "s3://$OBJECT_BUCKET/$MOST_RECENT" --recursive --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl; then
      error_exit "Failed to list backup contents at s3://$OBJECT_BUCKET/$MOST_RECENT" 103
    fi

    echo "Sync backup files from S3 to restore directory"
    if ! s3_retry aws s3 sync "s3://$OBJECT_BUCKET/$MOST_RECENT" "/backup/$RESTORE_DIR" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl; then
      error_exit "S3 sync failure from s3://$OBJECT_BUCKET/$MOST_RECENT to /backup/$RESTORE_DIR" 104
    fi

    echo "List backup files"
    ls -l "/backup/$RESTORE_DIR/."

    if [[ ! -s "/backup/$RESTORE_DIR/backup.sql" ]]; then
      error_exit "Database dump missing or empty at /backup/$RESTORE_DIR/backup.sql" 105
    fi

    # If backup available, scale down Quay
    echo "Scaling down Quay"
    QUAY_DEPLOYMENT=$(kubectl get deployment -n "$QUAY_NAMESPACE" -l quay-component=quay -o jsonpath='{.items[0].metadata.name}')
    REPLICAS=$(kubectl get deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" -o jsonpath='{.items[0].spec.replicas}')
    kubectl scale deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas=0

    restore_replicas() {
      if [[ -n "$REPLICAS" ]]; then
        echo "Restoring Quay replicas to $REPLICAS"
        kubectl scale deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas="$REPLICAS" || true
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
    kubectl exec -it pod/"$DB_POD_NAME" -n "$QUAY_NAMESPACE" -- sh -c '/usr/bin/dropdb --if-exists -U $POSTGRESQL_USER $POSTGRESQL_DATABASE'
    kubectl exec -it pod/"$DB_POD_NAME" -n "$QUAY_NAMESPACE" -- sh -c '/usr/bin/createdb -U $POSTGRESQL_USER $POSTGRESQL_DATABASE'
    kubectl exec -it pod/"$DB_POD_NAME" -n "$QUAY_NAMESPACE" -- sh -c '/usr/bin/psql -U $POSTGRESQL_USER $POSTGRESQL_DATABASE -f /backup/$RESTORE_DIR/backup.sql'

    echo "Scaling up Quay"
    kubectl scale deployment "$QUAY_DEPLOYMENT" -n "$QUAY_NAMESPACE" --replicas="$REPLICAS"

    echo "Delete local backup copies older than ${RETENTION_PERIOD} days"
    find /backup/* -type d -mtime +"${RETENTION_PERIOD}" -print -exec rm -rf {} +

    echo "Done"