# Régularité des TGV SNCF – pipeline Big Data sur Onyxia (Gr02)

## Auteurs
- LALI Mohamed Kenzi
- SOUEID Yassine

## Prérequis
- Un compte sur l'Onyxia de l'école (https://onyxia.adm.adaltas.cloud) et un service **Vscode-python** (image `inseefrlab/onyxia-vscode-python:py3.13.15`, 2 CPU / 2 Go RAM) avec le stockage S3 activé.
- Dans le service : Python 3.13, `aws` CLI, `git`, `curl` (déjà présents dans l'image). Les librairies Python (`duckdb`, `pandas`, `pyarrow`, `s3fs`, `polars`) sont installées par `src/run_all.sh` via `src/requirements.txt`.
- Accès internet sortant vers `ressources.data.sncf.com` et `github.com`.

## Jeu de données
- **Nom** : Régularité mensuelle TGV par liaisons (SNCF Voyageurs, données AQST)
- **URL** : https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/
- **Licence** : Open Database License (ODbL)
- **Téléchargé le** : 21/11/2026 (export CSV `;`, dernière mise à jour source : 02/10/2026)
- **Taille** : 2 909 048 octets (2,8 Mo), 12 907 lignes (liaison x mois, de 2018-01 à 2026-09), 26 colonnes

## Problématique
Quelles liaisons TGV ont vu leur ponctualité se dégrader le plus entre 2019 (avant Covid) et 2025 (dernière année complète) ?
L'indicateur est le taux de trains arrivés avec plus de 15 min de retard (trains en retard > 15 min / trains réellement circulés), calculé par liaison et par année.
On ne garde que les liaisons présentes les 12 mois des deux années avec au moins 1 000 trains par an.

## Chemins S3 (bucket `user-m-lali-ece`, endpoint `s3.seaweedfs.adm.adaltas.cloud`)
| Type | Chemin |
|---|---|
| Brut | `s3://user-m-lali-ece/bigdata_project/raw/regularite-mensuelle-tgv-aqst.csv` (+ `.sha256`) |
| Traité | `s3://user-m-lali-ece/bigdata_project/processed/` : `tgv_clean.parquet`, `degradation_liaisons.parquet` / `.csv` (**résultat final**), `quality_report.csv`, `taux_retard_mensuel.csv`, `benchmark.csv` |

## Reproduction
```bash
# 1. Dans le terminal du service Vscode-python
cd ~/work && git clone https://github.com/kenzi0228/bigdata_project.git && cd bigdata_project

# 2. (seulement si le service a plus de 12 h) jetons S3 expirés : dans Onyxia > Stockage de données >
#    roue crantée > "AWS CLI / shared profile", copier le script et le coller dans le terminal.
#    src/00_s3_env.sh recharge ensuite automatiquement les variables AWS_* depuis ~/.aws/credentials.

# 3. Tout le pipeline (environ 1 min) : ingestion -> nettoyage DuckDB -> vérification pandas -> benchmark
bash src/run_all.sh

# Relancer une seule étape (chaque étape écrase ses sorties, on peut relancer sans rien nettoyer)
cd src && . ./00_s3_env.sh
bash 01_ingest.sh          # télécharge le CSV SNCF et le dépose dans raw/ (crée le bucket si besoin)
python 02_transform.py     # contrôle qualité + nettoyage + résultat -> processed/
python 03_verify.py        # recalcul indépendant en pandas, s'arrête en erreur si écart > 0,01 point
python 04_benchmark.py data/regularite-mensuelle-tgv-aqst.csv   # pandas vs DuckDB vs Polars (x1 et x100)
```
Pour utiliser un autre bucket : `export BUCKET=<mon-bucket>` avant de lancer. Les captures d'exécution sont dans `evidence/`, les valeurs du service Kubernetes dans `k8s/`.
