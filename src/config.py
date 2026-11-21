"""Configuration commune : chemins S3 et connexion DuckDB."""
import os

# Sur Onyxia, le bucket personnel porte le nom de l'utilisateur (namespace "user-<id>")
BUCKET = os.environ.get("BUCKET") or os.environ.get("KUBERNETES_NAMESPACE", "user-local").replace("user-", "")
# ROOT permet de tester en local (ex: ROOT=/tmp/test) sans toucher a S3
ROOT = os.environ.get("ROOT", f"s3://{BUCKET}/bigdata_project")

RAW_CSV = f"{ROOT}/raw/regularite-mensuelle-tgv-aqst.csv"
PROCESSED = f"{ROOT}/processed"

YEAR_REF, YEAR_CMP = 2019, 2025      # avant Covid vs derniere annee complete
MIN_TRAINS = 1000                    # liaisons trop petites = trop bruitees


def duckdb_connect():
    import duckdb
    con = duckdb.connect()
    if ROOT.startswith("s3://"):
        con.execute("INSTALL httpfs").fetchall()
        con.execute("LOAD httpfs").fetchall()
        con.execute(f"""
            CREATE SECRET s3_onyxia (
                TYPE S3,
                KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
                SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
                SESSION_TOKEN '{os.environ.get("AWS_SESSION_TOKEN", "")}',
                ENDPOINT '{os.environ.get("AWS_S3_ENDPOINT", "minio.lab.sspcloud.fr")}',
                REGION '{os.environ.get("AWS_DEFAULT_REGION", "us-east-1")}',
                URL_STYLE 'path'
            );""").fetchall()
    return con


def s3fs_options():
    """Options pour lire S3 avec pandas (s3fs), utilise par la verification."""
    if not ROOT.startswith("s3://"):
        return None
    return {
        "key": os.environ["AWS_ACCESS_KEY_ID"],
        "secret": os.environ["AWS_SECRET_ACCESS_KEY"],
        "token": os.environ.get("AWS_SESSION_TOKEN"),
        "client_kwargs": {"endpoint_url": "https://" + os.environ.get("AWS_S3_ENDPOINT", "minio.lab.sspcloud.fr")},
    }
