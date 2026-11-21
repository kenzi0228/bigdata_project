#!/bin/bash
# Lance tout le pipeline de bout en bout (a executer sur Onyxia : bash src/run_all.sh)
# Chaque etape peut etre relancee seule : elles sont idempotentes (les fichiers S3 sont ecrases).
set -e
cd "$(dirname "$0")"
. ./00_s3_env.sh
echo "===== DEBUT $(date '+%F %T %Z') | namespace=$KUBERNETES_NAMESPACE ====="
pip install -q -r requirements.txt
bash 01_ingest.sh
python 02_transform.py
python 03_verify.py
python 04_benchmark.py data/regularite-mensuelle-tgv-aqst.csv
s3 cp data/benchmark.csv $S3_PREFIX/processed/benchmark.csv
s3 ls --recursive --human-readable $S3_PREFIX/
echo "===== FIN $(date '+%F %T %Z') ====="
