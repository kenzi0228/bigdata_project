#!/bin/bash
# Etape 1 : telechargement du CSV brut SNCF et depot sur le stockage S3 d'Onyxia
set -e
BUCKET=${BUCKET:-${KUBERNETES_NAMESPACE#user-}}
URL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/regularite-mensuelle-tgv-aqst/exports/csv?delimiter=%3B"
FILE=regularite-mensuelle-tgv-aqst.csv

echo "[$(date '+%F %T')] namespace=$KUBERNETES_NAMESPACE bucket=$BUCKET"
mkdir -p data
curl -sSL "$URL" -o data/$FILE
echo "Taille du fichier brut : $(du -h data/$FILE | cut -f1) ($(stat -c %s data/$FILE) octets)"
sha256sum data/$FILE | tee data/$FILE.sha256

mc cp data/$FILE      s3/$BUCKET/bigdata_project/raw/$FILE
mc cp data/$FILE.sha256 s3/$BUCKET/bigdata_project/raw/$FILE.sha256
mc ls --recursive s3/$BUCKET/bigdata_project/raw/
