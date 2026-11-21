"""Etape 4 : comparaison de 3 moteurs (pandas, DuckDB, Spark local) sur la meme agregation.

Mesure le temps de l'agregation "taux de retard > 15 min par liaison et par annee"
sur le fichier reel (x1) puis sur une copie repliquee 100 fois (x100) pour estimer
le comportement si le jeu de donnees etait 100 fois plus gros.
Usage : python 04_benchmark.py data/regularite-mensuelle-tgv-aqst.csv
"""
import os
import sys
import time

import duckdb
import pandas as pd

src = sys.argv[1] if len(sys.argv) > 1 else "data/regularite-mensuelle-tgv-aqst.csv"
big = "data/tgv_x100.csv"
N_RUNS = 3

# Fabrication du fichier x100 (on repete les lignes de donnees 100 fois)
if not os.path.exists(big):
    pd.concat([pd.read_csv(src, sep=";")] * 100).to_csv(big, sep=";", index=False)


def run_pandas(path):
    df = pd.read_csv(path, sep=";")
    df["annee"] = df["date"].str[:4]
    df["circ"] = df.nb_train_prevu - df.nb_annulation
    return df.groupby(["gare_depart", "gare_arrivee", "annee"])[["circ", "nb_train_retard_sup_15"]].sum()


def run_duckdb(path):
    return duckdb.sql(f"""
        SELECT gare_depart, gare_arrivee, left(date, 4) annee,
               sum(nb_train_prevu - nb_annulation) circ, sum(nb_train_retard_sup_15) r15
        FROM read_csv('{path}', delim=';', header=true) GROUP BY ALL""").df()


spark = None
try:
    from pyspark.sql import SparkSession, functions as F
    t = time.time()
    spark = SparkSession.builder.master("local[*]").appName("bench").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Demarrage de la session Spark : {time.time() - t:.1f} s")

    def run_spark(path):
        df = (spark.read.option("header", True).option("sep", ";").option("multiLine", True)
              .option("escape", '"').option("inferSchema", True).csv(path))
        return (df.withColumn("annee", F.substring("date", 1, 4))
                  .groupBy("gare_depart", "gare_arrivee", "annee")
                  .agg(F.sum(F.col("nb_train_prevu") - F.col("nb_annulation")).alias("circ"),
                       F.sum("nb_train_retard_sup_15").alias("r15"))
                  .toPandas())
except Exception as e:  # pyspark absent ou Java absent
    print(f"Spark indisponible ({e.__class__.__name__}), benchmark sans Spark")

engines = {"pandas": run_pandas, "duckdb": run_duckdb}
if spark:
    engines["spark"] = run_spark

lines = []
for label, path in [("x1", src), ("x100", big)]:
    size = os.path.getsize(path) / 1e6
    for name, fn in engines.items():
        times = []
        for _ in range(N_RUNS):
            t = time.time()
            fn(path)
            times.append(time.time() - t)
        med = sorted(times)[len(times) // 2]
        lines.append((label, f"{size:.1f} Mo", name, round(med, 3)))
        print(f"{label:5s} {size:7.1f} Mo  {name:7s} mediane sur {N_RUNS} runs = {med:.3f} s")

out = pd.DataFrame(lines, columns=["taille", "volume", "moteur", "temps_median_s"])
out.to_csv("data/benchmark.csv", index=False)
print(out.to_string(index=False))
