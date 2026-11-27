# Reproduction sur Onyxia par Yassine SOUEID

## Environnement

| Paramètre | Valeur observée |
|---|---|
| Namespace Kubernetes et bucket S3 | `user-y-soueid-ece` |
| Pod | `vscode-python-224609-0` |
| Préfixe S3 | `s3://user-y-soueid-ece/bigdata_project/` |
| Dépôt, révision au lancement | `kenzi0228/bigdata_project` · `d3ef19d` |
| Image du service | `inseefrlab/onyxia-vscode-python:py3.14.7` |
| Versions des moteurs | DuckDB 2.0.1 · pandas 3.1.1 · Polars 2.1.0 |

## Ingestion et contrôles

Le script `01_ingest.sh` a téléchargé le CSV **Régularité mensuelle TGV par liaisons** depuis l’API SNCF et l’a déposé avec son empreinte dans `raw/`. Le terminal affiche **2 909 048 octets** et **15 425 lignes physiques**. Le lecteur CSV de DuckDB dénombre **12 907 enregistrements**. Les deux compteurs mesurent respectivement les lignes de texte du fichier et les enregistrements CSV. [Capture 12](12_reproduction_yassine_ingestion_s3_raw.jpg) · [Capture 13](13_reproduction_yassine_transformation_duckdb.jpg)

**SHA-256 du CSV brut**  
`ea35772466b6c14a601bd9618ade2fd62af6620b4a91ece529ef65ccf8e5a559`

| Contrôle | Kenzi · 21 novembre | Yassine · 27 novembre |
|---|---:|---:|
| Taille du brut (octets) | 2 909 048 | 2 909 048 |
| Lignes brutes lues par DuckDB | 12 907 | 12 907 |
| Lignes exclues | 116 | 116 |
| Lignes propres après fusion National/International | 12 749 | 12 749 |
| Liaisons comparables entre 2019 et 2025 | 83 | 83 |
| Liaisons dont le taux de retard a augmenté | 66 | 66 |
| Liaisons identiques au recalcul pandas | 83 | 83 |
| Écart maximal entre pandas et DuckDB (point) | 0,0000 | 0,0000 |

Sortie du contrôle indépendant : `VERIFICATION OK : 83 liaisons identiques, ecart max = 0.0000 point`. Le préfixe S3 comptait ensuite **8 objets** pour un total affiché de **2,9 MiB**, dont le CSV brut, son empreinte et les six fichiers traités. [Capture 14](14_reproduction_yassine_verification.jpg) · [Capture 16](16_reproduction_yassine_contenu_bucket_s3.jpg)

## Cinq plus fortes dégradations

Les taux portent sur les trains arrivés avec plus de 15 minutes de retard. L’évolution en points est celle enregistrée dans le Parquet, avant arrondi des taux affichés. [Capture 17](17_reproduction_yassine_comparaison_resultats.jpg)

| Liaison | Taux 2019 observé | Taux 2025 observé | Évolution observée | Évolution de référence |
|---|---:|---:|---:|---:|
| PARIS EST → STUTTGART | 15,62 % | 56,54 % | +40,93 pts | +40,93 pts |
| PARIS EST → FRANCFORT | 14,00 % | 31,56 % | +17,56 pts | +17,56 pts |
| MACON LOCHE → PARIS LYON | 11,00 % | 25,19 % | +14,19 pts | +14,19 pts |
| GRENOBLE → PARIS LYON | 6,35 % | 15,06 % | +8,71 pts | +8,71 pts |
| LILLE → MARSEILLE ST CHARLES | 14,20 % | 21,48 % | +7,28 pts | +7,28 pts |

## Benchmark dans le pod de Yassine

Temps médians sur trois exécutions, en secondes. Le script a écrit les mesures dans `processed/benchmark.csv`. [Capture 15](15_reproduction_yassine_benchmark.jpg)

| Moteur | ×1 · 2,9 Mo | ×100 · 289,6 Mo |
|---|---:|---:|
| pandas | 0,031 s | 1,344 s |
| DuckDB | 0,176 s | 1,820 s |
| Polars | 0,145 s | 1,138 s |

## Conclusion

La reproduction sur le namespace de Yassine retrouve les 83 liaisons comparables, dont 66 dégradées, ainsi que le classement et les taux de référence des cinq premières liaisons. Le recalcul indépendant avec pandas confirme les résultats de DuckDB pour les 83 liaisons, avec un écart maximal de 0 point.
