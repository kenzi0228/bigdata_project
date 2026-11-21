#!/bin/bash
# A sourcer (". 00_s3_env.sh") : variables communes + rafraichissement des identifiants S3.
# Les jetons S3 d'Onyxia expirent (12 h). Si le service est ancien, coller le script
# "AWS CLI / shared profile" d'Onyxia (Stockage de donnees > roue crantee) qui ecrit
# ~/.aws/credentials, puis sourcer ce fichier pour recharger les variables d'environnement.
if [ -f ~/.aws/credentials ]; then
  export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
  export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
  export AWS_SESSION_TOKEN=$(aws configure get aws_session_token)
fi
export AWS_S3_ENDPOINT=${AWS_S3_ENDPOINT:-s3.seaweedfs.adm.adaltas.cloud}
export BUCKET=${BUCKET:-$KUBERNETES_NAMESPACE}
export S3_PREFIX="s3://$BUCKET/bigdata_project"
s3() { aws --endpoint-url "https://$AWS_S3_ENDPOINT" s3 "$@"; }
