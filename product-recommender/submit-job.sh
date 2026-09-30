#!/usr/bin/env bash
#
# Submits the recsys-trainer job to the odctl Flink session cluster.
#
#   1. uploads the bootstrap CSV to SeaweedFS, so the split enumerator on the
#      JobManager and the readers on every TaskManager see one copy
#   2. copies the fat JAR into the JobManager, where `flink run` executes main()
#   3. submits the job detached
#
# Assumes `odctl up kafka-lite flink-full valkey` is already running.

set -euo pipefail

cd "$(dirname "$0")"

JAR="recsys-trainer/build/libs/recsys-trainer-1.0.jar"
CSV="data/training_log.csv"
JOBMANAGER="flink-jobmanager"
# The filer, not the S3 gateway on 8333: it takes a plain multipart POST and
# needs no SigV4 signing. SeaweedFS maps S3 buckets to /buckets/<name>.
FILER="http://127.0.0.1:8889/buckets/odctl-dev/recsys/"

if [[ ! -f "$JAR" ]]; then
  echo "JAR not found at $JAR. Run: (cd recsys-trainer && ./gradlew shadowJar)" >&2
  exit 1
fi

if [[ ! -f "$CSV" ]]; then
  echo "Bootstrap CSV not found at $CSV. Run: python -m recommender.run.prepare" >&2
  exit 1
fi

echo "==> Uploading $(basename "$CSV") to SeaweedFS"
curl -sf -F "file=@${CSV}" "$FILER" > /dev/null
echo "    s3://odctl-dev/recsys/$(basename "$CSV")"

echo "==> Copying $(basename "$JAR") into $JOBMANAGER"
docker cp "$JAR" "${JOBMANAGER}:/tmp/$(basename "$JAR")"

echo "==> Submitting job"
docker exec "$JOBMANAGER" flink run -d "/tmp/$(basename "$JAR")"

echo
echo "Flink UI:  http://127.0.0.1:8082"
echo "Kafka UI:  http://127.0.0.1:8086"
