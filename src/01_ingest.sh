#!/bin/bash
# Etape 1 : telechargement du CSV brut SNCF et depot sur le stockage S3 d'Onyxia
set -e
cd "$(dirname "$0")"
. ./00_s3_env.sh
URL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-aqst/exports/csv?delimiter=%3B"
FILE=regularite-mensuelle-tgv-aqst.csv

echo "[$(date '+%F %T %Z')] namespace=$KUBERNETES_NAMESPACE bucket=$BUCKET"
mkdir -p data
curl -sSL "$URL" -o data/$FILE
echo "Taille du fichier brut : $(du -h data/$FILE | cut -f1) ($(stat -c %s data/$FILE) octets), $(wc -l < data/$FILE) lignes physiques"
sha256sum data/$FILE | tee data/$FILE.sha256

s3 cp data/$FILE        $S3_PREFIX/raw/$FILE
s3 cp data/$FILE.sha256 $S3_PREFIX/raw/$FILE.sha256
s3 ls --recursive --human-readable $S3_PREFIX/raw/
