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

    BACKUP_DIR="quay-backup-$(date +%Y%m%d)"

    echo "Create backup directory $BACKUP_DIR"
    if ! mkdir -p "/backup/$BACKUP_DIR"; then
      error_exit "Failed to create backup directory /backup/$BACKUP_DIR" 102
    fi    

    echo "Extract resource details"
    REGISTRY_NAME=$(kubectl get quayregistry -n $QUAY_NAMESPACE -o jsonpath='{.items[0].metadata.name}')
    CONFIG_BUNDLE_SECRET=$(kubectl get quayregistry $REGISTRY_NAME -n $QUAY_NAMESPACE -o jsonpath='{.spec.configBundleSecret}')
    APP_POD_NAME=$(kubectl get pod -l app=quay -n $QUAY_NAMESPACE -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$APP_POD_NAME" ]]; then
      error_exit "Failed to get Quay app pod name in namespace $QUAY_NAMESPACE" 201
    fi
    DB_POD_NAME=$(kubectl get pod -l quay-component=postgres -n $QUAY_NAMESPACE -o jsonpath='{.items[0].metadata.name}')
    if [[ -z "$DB_POD_NAME" ]]; then
      error_exit "Failed to get Postgres pod name in namespace $QUAY_NAMESPACE" 202
    fi

    echo "Capture backup of the Quay configuration"
    kubectl neat get -- quayregistry $REGISTRY_NAME -n $QUAY_NAMESPACE -o yaml > "/backup/$BACKUP_DIR/quay-registry.yaml"
    kubectl neat get -- secret $REGISTRY_NAME-quay-registry-managed-secret-keys -n $QUAY_NAMESPACE -o yaml > "/backup/$BACKUP_DIR/managed-secret-keys.yaml"
    kubectl exec -it $APP_POD_NAME -n $QUAY_NAMESPACE -- cat /conf/stack/config.yaml > "/backup/$BACKUP_DIR/quay-config.yaml"
    kubectl neat get -- secret $CONFIG_BUNDLE_SECRET -n $QUAY_NAMESPACE -o yaml > "/backup/$BACKUP_DIR/config-bundle.yaml"

    echo "Perform a Quay database backup"
    kubectl exec -it pod/$DB_POD_NAME -n $QUAY_NAMESPACE -- sh -c ' /usr/bin/pg_dump -C $POSTGRESQL_DATABASE' > "/backup/$BACKUP_DIR/backup.sql"

    if [[ ! -s "/backup/$BACKUP_DIR/backup.sql" ]]; then
      error_exit "Database dump missing or empty at /backup/$BACKUP_DIR/backup.sql" 105
    fi

    echo "List backup files"
    ls -l /backup/$BACKUP_DIR/.
    
    echo "Sync backup files to S3 bucket"
    if ! s3_retry aws s3 sync "/backup/$BACKUP_DIR/." "s3://$OBJECT_BUCKET/$BACKUP_DIR" --endpoint-url "https://$S3_ENDPOINT" --expires "$(date -I -d '7 days')" --no-verify-ssl; then
      error_exit "S3 sync failure from /backup/$BACKUP_DIR to s3://$OBJECT_BUCKET/$BACKUP_DIR" 104
    fi

    echo "List bucket contents"
    if ! s3_retry aws s3 ls "s3://$OBJECT_BUCKET/$BACKUP_DIR" --endpoint-url "https://$S3_ENDPOINT" --no-verify-ssl; then
      error_exit "Failed to list backup contents at s3://$OBJECT_BUCKET/$BACKUP_DIR" 103
    fi 

    echo "Delete local backup copies older than ${RETENTION_PERIOD} days"
    find /backup/* -type d -mtime +"${RETENTION_PERIOD}" -print -exec rm -rf {} +

    echo "Done"
