"""Etape 3 : verification independante du resultat.

On recalcule le resultat a partir du CSV BRUT avec un autre outil (pandas, code ecrit
separement, sans reutiliser le SQL DuckDB), puis on compare avec le fichier produit
par le pipeline (degradation_liaisons.parquet). Le script s'arrete en erreur si un ecart
> 0.01 point est trouve.
"""
from datetime import datetime

import pandas as pd

from config import RAW_CSV, PROCESSED, YEAR_REF, YEAR_CMP, MIN_TRAINS, s3fs_options

opts = s3fs_options()
print(f"[{datetime.now():%F %T}] Verification pandas a partir de {RAW_CSV}")
df = pd.read_csv(RAW_CSV, sep=";", storage_options=opts)

counts = ["nb_train_prevu", "nb_annulation", "nb_train_depart_retard", "nb_train_retard_arrivee",
          "nb_train_retard_sup_15", "nb_train_retard_sup_30", "nb_train_retard_sup_60"]
ok = (df.nb_train_prevu > 0) & (df.nb_annulation <= df.nb_train_prevu) & (df[counts] >= 0).all(axis=1)
df = df[ok].copy()
df["annee"] = df["date"].str[:4].astype(int)
df["circules"] = df.nb_train_prevu - df.nb_annulation
df = df[df.annee.isin([YEAR_REF, YEAR_CMP])]

g = (df.groupby(["gare_depart", "gare_arrivee", "annee"])
       .agg(mois=("date", "nunique"), trains=("circules", "sum"), r15=("nb_train_retard_sup_15", "sum"))
       .reset_index())
g = g[(g.mois == 12) & (g.trains >= MIN_TRAINS)]
g["taux"] = 100 * g.r15 / g.trains
p = g.pivot(index=["gare_depart", "gare_arrivee"], columns="annee", values="taux").dropna()
p["evolution_pandas"] = (p[YEAR_CMP] - p[YEAR_REF]).round(2)
p = p.reset_index()

res = pd.read_parquet(f"{PROCESSED}/degradation_liaisons.parquet", storage_options=opts)
m = res.merge(p[["gare_depart", "gare_arrivee", "evolution_pandas"]], on=["gare_depart", "gare_arrivee"], how="outer")

print(f"Liaisons pipeline : {len(res)} | liaisons pandas : {len(p)}")
m["ecart"] = (m.evolution_points - m.evolution_pandas).abs()
print(m.sort_values("evolution_points", ascending=False)
       [["gare_depart", "gare_arrivee", "evolution_points", "evolution_pandas", "ecart"]].head(5).to_string(index=False))

assert len(res) == len(p), "nombre de liaisons different"
assert m.ecart.max() <= 0.01, f"ecart max trop grand : {m.ecart.max()}"
print(f"\nVERIFICATION OK : {len(m)} liaisons identiques, ecart max = {m.ecart.max():.4f} point")
