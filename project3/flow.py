"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> marts -> one chart.

  python flow.py                # full data: data/raw  -> bluesky.duckdb, charts/replies_by_language.png
  python flow.py data/sample    # the sample -> bluesky_sample.duckdb, charts/replies_by_language_sample.png

Adapted from the class quakes_flow.py. stage(), entities() and the flow
function follow the Session 3 worksheet, Appendix A (Bluesky versions).
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

# A1: settings
SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))  # "raw" or "sample"
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb"  # bluesky_sample.duckdb for the sample
CHART = f"charts/replies_by_language{TAG}.png"  # the sample never overwrites my chart

TOP_N = 8  # languages shown in the chart (the 8 with the most posts)
LANG_NAMES = {"en": "English", "ja": "Japanese", "pt": "Portuguese", "de": "German",
              "es": "Spanish", "ko": "Korean", "fr": "French", "zh": "Chinese",
              "it": "Italian", "nl": "Dutch", "tr": "Turkish", "th": "Thai",
              "ru": "Russian", "pl": "Polish", "uk": "Ukrainian", "ca": "Catalan"}


@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of the .duckdb file
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
              AND kind = 'commit'                -- not identity or account events
              AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records")}


@task
def entities():
    """Kind 2: one row per post (did, rkey), with its latest operation."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                   make_timestamp(time_us) AS event_time,                  -- microseconds -> UTC
                   (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                       IS NOT NULL AS is_reply                             -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey
                                       ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return con.execute("SELECT count(*) FROM posts").fetchone()[0]


@task
def marts():
    """Comparison: posts and share of replies per language (created posts only)."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE mart_replies_by_language AS
            SELECT coalesce(lang, 'none') AS lang,
                   count(*) AS posts,
                   count(*) FILTER (WHERE is_reply) AS replies,
                   round(100.0 * count(*) FILTER (WHERE is_reply) / count(*), 1) AS pct_replies,
                   round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_of_all_posts
            FROM posts
            WHERE operation = 'create'
            GROUP BY lang
            ORDER BY posts DESC
        """)
        return con.execute("SELECT count(*) FROM mart_replies_by_language").fetchone()[0]


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute(f"""
            SELECT lang, posts, pct_replies
            FROM mart_replies_by_language
            WHERE lang <> 'none'
            ORDER BY posts DESC
            LIMIT {TOP_N}
        """).fetchall()
        start, end = con.execute(
            "SELECT min(event_time), max(event_time) FROM posts WHERE operation = 'create'"
        ).fetchone()
        overall = con.execute(
            "SELECT round(100.0 * avg(is_reply::INT), 1) FROM posts WHERE operation = 'create'"
        ).fetchone()[0]
    rows.sort(key=lambda r: r[2])  # highest reply share at the top of the chart
    labels = [f"{LANG_NAMES.get(r[0], r[0])} ({r[1]:,} posts)" for r in rows]
    shares = [r[2] for r in rows]
    top = rows[-1]
    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(9, 5))
    plt.barh(labels, shares, color="#2a7fb8")
    plt.axvline(overall, color="grey", linestyle="--", label=f"all languages: {overall}%")
    plt.title(f"{LANG_NAMES.get(top[0], top[0])} posts are replies most often ({top[2]}%)\n"
              f"Bluesky, {TOP_N} languages with the most posts, "
              f"{start:%d %b %H:%M}-{end:%H:%M} UTC (event time)", fontsize=11)
    plt.xlabel("share of posts that are replies (%)")
    plt.ylabel(f"language (top {TOP_N} by number of posts)")
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    return CHART


@flow(name="bluesky-pipeline")
def pipeline():
    log = get_run_logger()
    raw = load_raw()
    n = stage()
    things = entities()
    marts()
    path = chart()
    log.info("Raw records landed: %d", raw)
    log.info("Malformed (kept in raw): %d", n["malformed"])
    log.info("Unique Kafka records: %d", n["records"])
    log.info("Unique posts: %d", things)
    log.info("Chart: %s", path)


if __name__ == "__main__":
    pipeline()