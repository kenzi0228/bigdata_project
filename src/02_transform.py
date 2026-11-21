"""Etape 2 : controle qualite, nettoyage et agregation avec DuckDB.

Lit le CSV brut sur S3, produit sur S3 (prefixe processed/) :
  - quality_report.csv          : nombre de lignes touchees par chaque probleme
  - tgv_clean.parquet           : donnees nettoyees (une ligne = liaison x mois)
  - degradation_liaisons.parquet/.csv : resultat final (reponse a la question)
  - taux_retard_mensuel.csv     : serie nationale mensuelle (bonus pour les graphes)
"""
import time
from datetime import datetime

from config import RAW_CSV, PROCESSED, YEAR_REF, YEAR_CMP, MIN_TRAINS, duckdb_connect

t0 = time.time()
print(f"[{datetime.now():%F %T}] Lecture de {RAW_CSV}")
con = duckdb_connect()


def q(sql, params=None):
    """Execute une requete et recupere tout le resultat (obligatoire avec DuckDB >= 2)."""
    return con.execute(sql, params or []).fetchall()


# Le CSV contient des commentaires sur plusieurs lignes (entre guillemets) -> parser strict
q(f"""
    CREATE TABLE raw AS
    SELECT * FROM read_csv('{RAW_CSV}', delim=';', header=true, quote='"',
                           escape='"', strict_mode=false, all_varchar=false)
""")
n_raw = q("SELECT count(*) FROM raw")[0][0]
print(f"Lignes brutes : {n_raw}")

# ---------- 1. Controle qualite ----------
checks = {
    "commentaire_annulation vide": "commentaire_annulation IS NULL",
    "commentaire_retards_depart vide": "commentaire_retards_depart IS NULL",
    "commentaires_retard_arrivee vide": "commentaires_retard_arrivee IS NULL",
    "nb_train_prevu = 0": "nb_train_prevu = 0",
    "annulations > trains prevus": "nb_annulation > nb_train_prevu",
    "compteur negatif (nb_train_*)": "least(nb_train_prevu, nb_annulation, nb_train_depart_retard, "
                                     "nb_train_retard_arrivee, nb_train_retard_sup_15, "
                                     "nb_train_retard_sup_30, nb_train_retard_sup_60) < 0",
    "retard moyen des trains en retard negatif": "retard_moyen_arrivee < 0",
    "somme des % de causes hors 100 +-1": "abs(prct_cause_externe + prct_cause_infra + prct_cause_gestion_trafic"
                                          " + prct_cause_materiel_roulant + prct_cause_gestion_gare"
                                          " + prct_cause_prise_en_charge_voyageurs - 100) > 1",
}
rows = []
for name, cond in checks.items():
    n = q(f"SELECT count(*) FROM raw WHERE {cond}")[0][0]
    rows.append((name, n))
# doublons sur la cle (mois, depart, arrivee) : liaison decoupee National / International
n_dup = q("""
    SELECT coalesce(sum(c - 1), 0) FROM (
        SELECT count(*) c FROM raw GROUP BY date, gare_depart, gare_arrivee HAVING count(*) > 1)
""")[0][0]
rows.append(("doublon (mois, depart, arrivee)", n_dup))

q("CREATE TABLE quality(probleme VARCHAR, nb_lignes BIGINT)")
for r in rows:
    q("INSERT INTO quality VALUES (?, ?)", list(r))
print(con.sql("SELECT * FROM quality").df().to_string(index=False))

# ---------- 2. Nettoyage ----------
# - on supprime les 3 colonnes de commentaires (quasi vides, texte libre)
# - on exclut les lignes incoherentes (0 train prevu, annulations > prevus, compteurs negatifs)
# - on fusionne National + International d'une meme liaison (somme des compteurs)
q("""
    CREATE TABLE clean AS
    SELECT CAST(date || '-01' AS DATE)       AS mois,
           trim(gare_depart)                 AS gare_depart,
           trim(gare_arrivee)                AS gare_arrivee,
           sum(nb_train_prevu)               AS nb_train_prevu,
           sum(nb_annulation)                AS nb_annulation,
           sum(nb_train_prevu - nb_annulation) AS nb_train_circule,
           sum(nb_train_retard_arrivee)      AS nb_train_retard_arrivee,
           sum(nb_train_retard_sup_15)       AS nb_train_retard_sup_15,
           sum(nb_train_retard_sup_30)       AS nb_train_retard_sup_30,
           sum(nb_train_retard_sup_60)       AS nb_train_retard_sup_60
    FROM raw
    WHERE nb_train_prevu > 0
      AND nb_annulation <= nb_train_prevu
      AND least(nb_train_prevu, nb_annulation, nb_train_depart_retard, nb_train_retard_arrivee,
                nb_train_retard_sup_15, nb_train_retard_sup_30, nb_train_retard_sup_60) >= 0
    GROUP BY ALL
""")
n_excl = q("""SELECT count(*) FROM raw WHERE NOT (nb_train_prevu > 0
      AND nb_annulation <= nb_train_prevu
      AND least(nb_train_prevu, nb_annulation, nb_train_depart_retard, nb_train_retard_arrivee,
                nb_train_retard_sup_15, nb_train_retard_sup_30, nb_train_retard_sup_60) >= 0)""")[0][0]
n_clean = q("SELECT count(*) FROM clean")[0][0]
q("INSERT INTO quality VALUES ('lignes exclues (incoherentes)', ?), ('lignes apres nettoyage', ?)",
            [n_excl, n_clean])
print(f"Lignes exclues : {n_excl} | lignes propres (apres fusion National/International) : {n_clean}")

# ---------- 3. Resultat : evolution du taux de retard > 15 min par liaison ----------
q(f"""
    CREATE TABLE resultat AS
    WITH par_an AS (
        SELECT gare_depart, gare_arrivee, year(mois) AS annee,
               count(*) AS nb_mois,
               sum(nb_train_circule) AS trains,
               sum(nb_train_retard_sup_15) AS retards15
        FROM clean WHERE year(mois) IN ({YEAR_REF}, {YEAR_CMP})
        GROUP BY ALL
    )
    SELECT a.gare_depart, a.gare_arrivee,
           a.trains AS trains_{YEAR_REF}, b.trains AS trains_{YEAR_CMP},
           round(100.0 * a.retards15 / a.trains, 2) AS taux_retard15_{YEAR_REF},
           round(100.0 * b.retards15 / b.trains, 2) AS taux_retard15_{YEAR_CMP},
           round(100.0 * b.retards15 / b.trains - 100.0 * a.retards15 / a.trains, 2) AS evolution_points
    FROM par_an a JOIN par_an b USING (gare_depart, gare_arrivee)
    WHERE a.annee = {YEAR_REF} AND b.annee = {YEAR_CMP}
      AND a.nb_mois = 12 AND b.nb_mois = 12
      AND a.trains >= {MIN_TRAINS} AND b.trains >= {MIN_TRAINS}
    ORDER BY evolution_points DESC
""")
n_res = q("SELECT count(*) FROM resultat")[0][0]
print(f"\nLiaisons comparables {YEAR_REF} vs {YEAR_CMP} : {n_res}")
print("Top 10 des liaisons qui se sont le plus degradees :")
print(con.sql("SELECT * FROM resultat LIMIT 10").df().to_string(index=False))

q("""
    CREATE TABLE mensuel AS
    SELECT mois, sum(nb_train_circule) AS trains,
           round(100.0 * sum(nb_train_retard_sup_15) / sum(nb_train_circule), 2) AS taux_retard15
    FROM clean GROUP BY mois ORDER BY mois
""")

# ---------- 4. Ecriture sur S3 ----------
q(f"COPY quality  TO '{PROCESSED}/quality_report.csv' (HEADER)")
q(f"COPY clean    TO '{PROCESSED}/tgv_clean.parquet' (FORMAT parquet)")
q(f"COPY resultat TO '{PROCESSED}/degradation_liaisons.parquet' (FORMAT parquet)")
q(f"COPY resultat TO '{PROCESSED}/degradation_liaisons.csv' (HEADER)")
q(f"COPY mensuel  TO '{PROCESSED}/taux_retard_mensuel.csv' (HEADER)")
print(f"\n[{datetime.now():%F %T}] Fichiers ecrits dans {PROCESSED}/ en {time.time() - t0:.1f} s")
