## Quay Backup and Restore

Scripts to back up and restore Red Hat Quay using an S3-compatible object store and a running Kubernetes/OpenShift cluster.

### Prerequisites
- AWS CLI configured to access the target S3-compatible endpoint
- kubectl access to the Quay namespace
- kubectl-neat plugin installed (used by the backup script)
- Required environment variables set (see below)

### Environment variables
- `OBJECT_BUCKET`: S3 bucket name used to store backups
- `S3_ENDPOINT`: S3 endpoint host (the scripts use `https://$S3_ENDPOINT`)
- `QUAY_NAMESPACE`: Kubernetes namespace where Quay is deployed
- `RETENTION_PERIOD`: Number of days to keep local backup copies

### backup.sh behavior
- Creates a directory named `quay-backup-YYYYMMDD` under `/backup`.
- Captures Quay configuration resources and secrets (neat YAML) into the backup directory.
- Copies `/conf/stack/config.yaml` from the Quay app pod into the backup directory.
- Dumps the Quay Postgres database to `backup.sql` via the Postgres pod.
- Lists backup files locally, then syncs the directory to `s3://$OBJECT_BUCKET/$BACKUP_DIR`.
- Lists objects in the backup path on S3.
- Deletes local backup directories older than `RETENTION_PERIOD` days.
- On failure, exits with an error code (see table).

Run: `bash backup.sh`

### restore.sh behavior
- Verifies connectivity to the S3 bucket.
- Finds the most recent backup prefix in the bucket.
- Creates a matching local directory under `/backup` and syncs objects from S3 into it.
- Scales the Quay deployment down to 0 replicas.
- Recreates the Postgres database and restores from `/backup/$RESTORE_DIR/backup.sql` in the Postgres pod.
- Scales the Quay deployment back up to its previous replica count.
- Deletes local backup directories older than `RETENTION_PERIOD` days.
- On failure, exits with an error code (see table).

Run: `bash restore.sh`

### Error codes

Codes are grouped by domain: 1xx = S3/Object storage, 2xx = Kubernetes/namespace/pods.

| Code | Script(s)   | Meaning |
|------|-------------|---------|
| 100  | backup, restore | Unable to connect to S3 (check endpoint, credentials, and network) |
| 101  | restore      | No backups found in S3 bucket |
| 102  | backup, restore | Failed to create local backup/restore directory |
| 103  | backup, restore | Failed to list backup contents on S3 |
| 104  | backup, restore | S3 sync failure |
| 105  | backup, restore | Database dump `backup.sql` missing or empty in local backup directory |
| 200  | backup, restore | Namespace `$QUAY_NAMESPACE` does not exist or is not accessible |
| 201  | backup       | Failed to get Quay app pod name in the target namespace |
| 202  | backup, restore | Failed to get Postgres pod name in the target namespace |
| 203  | restore      | Timed out waiting for Quay pods to terminate after scaling down |
| 204  | restore      | Failed to scale down Quay deployment |
| 205  | restore      | Failed to scale Quay deployment back to the original replica count |
| 206  | restore      | Failed to get Quay deployment name in the target namespace |
| 207  | restore      | Failed to drop database in the Postgres pod |
| 208  | restore      | Failed to create database in the Postgres pod |
| 209  | restore      | Failed to restore database from backup.sql in the Postgres pod |
| 210  | restore      | Failed to extract quay-registry-hostname from managed-secret-keys.yaml |
| 211  | restore      | managed-secret-keys.yaml not found in local backup directory |
| 212  | restore      | Failed to derive source cluster from quay-registry-hostname |

Notes:
- The scripts use `set -o pipefail` and exit non-zero on handled errors via a common `error_exit` helper.
- Other failures from `kubectl`, `aws`, or shell commands will also cause non-zero exits, even if not mapped to a specific code.
