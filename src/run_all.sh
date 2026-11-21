#!/bin/bash
# Lance tout le pipeline de bout en bout (a executer depuis la racine du depot sur Onyxia)
set -e
cd "$(dirname "$0")"
echo "===== $(date '+%F %T') | namespace=$KUBERNETES_NAMESPACE ====="
pip install -q -r requirements.txt
bash 01_ingest.sh
python 02_transform.py
python 03_verify.py
python 04_benchmark.py data/regularite-mensuelle-tgv-aqst.csv
BUCKET=${BUCKET:-${KUBERNETES_NAMESPACE#user-}}
mc cp data/benchmark.csv s3/$BUCKET/bigdata_project/processed/benchmark.csv
mc ls --recursive s3/$BUCKET/bigdata_project/
echo "===== FIN $(date '+%F %T') ====="
