# import requests
# import psycopg2
# import time
# import logging
# import signal
# import sys
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from psycopg2.extras import execute_values
# from datetime import datetime, timedelta
# from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.triggers.cron import CronTrigger
# from config.config import CMOTS_TOKEN, DB_CONFIG

# # ─── CONFIG ────────────────────────────────────────────────────────────────────

# API_URL   = "https://equifizapis.cmots.com/api/CompanyMaster"
# JWT_TOKEN = CMOTS_TOKEN
# DB_CONFIG = DB_CONFIG

# PAGE_SIZE        = 10
# REQUEST_DELAY    = 0.3   # seconds between API calls
# MAX_EMPTY_PAGES  = 3     # stop after N consecutive empty pages

# # ─── SCHEDULE CONFIG ──────────────────────────────────────────────────────────
# # Runs twice a day: 6:00 AM and 6:00 PM IST
# # Change to SCHEDULE_MODE = "once" and set SCHEDULE_HOUR for once-a-day
# SCHEDULE_MODE    = "twice"   # "once" | "twice"
# SCHEDULE_HOUR_1  = 6         # First run  → 06:00
# SCHEDULE_HOUR_2  = 18        # Second run → 18:00  (ignored if mode = "once")
# TIMEZONE         = "Asia/Kolkata"

# # ─── LOGGING ─────────────────────────────────────────────────────────────────

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     handlers=[
#         logging.StreamHandler(sys.stdout),
#         logging.FileHandler("company_sync.log"),
#     ],
# )
# log = logging.getLogger(__name__)

# # ─── SQL ──────────────────────────────────────────────────────────────────────

# SQL_CREATE_TABLE = """
# CREATE TABLE IF NOT EXISTS companies (
#     co_code          INTEGER PRIMARY KEY,
#     bsecode          VARCHAR(50),
#     nsesymbol        VARCHAR(50),
#     companyname      VARCHAR(255),
#     companyshortname VARCHAR(100),
#     categoryname     VARCHAR(100),
#     isin             VARCHAR(50),
#     bsegroup         VARCHAR(50),
#     mcaptype         VARCHAR(50),
#     sectorcode       VARCHAR(50),
#     sectorname       VARCHAR(100),
#     industrycode     VARCHAR(50),
#     industryname     VARCHAR(100),
#     bselistedflag    VARCHAR(10),
#     nselistedflag    VARCHAR(10),
#     displaytype      VARCHAR(50),
#     synced_at        TIMESTAMP DEFAULT NOW()
# );
# """

# SQL_CREATE_SYNC_LOG = """
# CREATE TABLE IF NOT EXISTS sync_log (
#     id            SERIAL PRIMARY KEY,
#     run_at        TIMESTAMP DEFAULT NOW(),
#     pages_fetched INTEGER,
#     rows_upserted INTEGER,
#     new_companies INTEGER,
#     duration_secs FLOAT,
#     status        VARCHAR(20),
#     notes         TEXT
# );
# """

# SQL_UPSERT = """
# INSERT INTO companies (
#     co_code, bsecode, nsesymbol, companyname, companyshortname,
#     categoryname, isin, bsegroup, mcaptype, sectorcode, sectorname,
#     industrycode, industryname, bselistedflag, nselistedflag,
#     displaytype, synced_at
# ) VALUES %s
# ON CONFLICT (co_code) DO UPDATE SET
#     bsecode          = EXCLUDED.bsecode,
#     nsesymbol        = EXCLUDED.nsesymbol,
#     companyname      = EXCLUDED.companyname,
#     companyshortname = EXCLUDED.companyshortname,
#     categoryname     = EXCLUDED.categoryname,
#     isin             = EXCLUDED.isin,
#     bsegroup         = EXCLUDED.bsegroup,
#     mcaptype         = EXCLUDED.mcaptype,
#     sectorcode       = EXCLUDED.sectorcode,
#     sectorname       = EXCLUDED.sectorname,
#     industrycode     = EXCLUDED.industrycode,
#     industryname     = EXCLUDED.industryname,
#     bselistedflag    = EXCLUDED.bselistedflag,
#     nselistedflag    = EXCLUDED.nselistedflag,
#     displaytype      = EXCLUDED.displaytype,
#     synced_at        = EXCLUDED.synced_at;
# """

# # ─── HELPERS ─────────────────────────────────────────────────────────────────

# def get_session():
#     session = requests.Session()
#     retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount("https://", HTTPAdapter(max_retries=retry))
#     return session

# def fetch_page(session, page_no):
#     headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
#     params  = {"PageNo": page_no}
#     try:
#         resp = session.get(API_URL, headers=headers, params=params, timeout=30)
#         resp.raise_for_status()
#         data = resp.json()
#         if isinstance(data, list):
#             return data
#         for key in ("data", "Data", "result", "Result"):
#             if key in data and isinstance(data[key], list):
#                 return data[key]
#         return []
#     except Exception as e:
#         log.warning(f"Page {page_no} fetch error: {e}")
#         return None

# def to_row(c, synced_at):
#     def g(*keys):
#         for k in keys:
#             v = c.get(k)
#             if v is not None:
#                 return v
#         return ""
#     return (
#         g("co_code", "CoCode"),
#         g("bsecode", "BseCode"),
#         g("nsesymbol", "NseSymbol"),
#         g("companyname", "CompanyName"),
#         g("companyshortname", "CompanyShortName"),
#         g("categoryname", "CategoryName"),
#         g("isin", "Isin"),
#         g("bsegroup", "BseGroup"),
#         g("mcaptype", "McapType"),
#         g("sectorcode", "SectorCode"),
#         g("sectorname", "SectorName"),
#         g("industrycode", "IndustryCode"),
#         g("industryname", "IndustryName"),
#         g("bselistedflag", "BseListedFlag"),
#         g("nselistedflag", "NseListedFlag"),
#         g("displaytype", "DisplayType"),
#         synced_at,
#     )

# def db_connect():
#     return psycopg2.connect(**DB_CONFIG)

# def ensure_tables(cur):
#     cur.execute(SQL_CREATE_TABLE)
#     cur.execute(SQL_CREATE_SYNC_LOG)

# def log_run(cur, pages, upserted, new_cos, duration, status, notes=""):
#     cur.execute(
#         """INSERT INTO sync_log (pages_fetched, rows_upserted, new_companies, duration_secs, status, notes)
#            VALUES (%s, %s, %s, %s, %s, %s)""",
#         (pages, upserted, new_cos, duration, status, notes),
#     )

# # ─── CORE SYNC JOB ───────────────────────────────────────────────────────────

# def sync_companies():
#     """
#     Smart incremental sync:
#       1. Check current DB count → derive resume page.
#       2. Fetch from that page onward.
#       3. Stop when MAX_EMPTY_PAGES consecutive empty responses are seen.
#       4. Upsert everything (handles both new and updated records).
#       5. Write a run summary to sync_log.
#     """
#     run_start   = time.time()
#     log.info("=" * 60)
#     log.info("Starting company sync job")

#     pages_fetched = 0
#     rows_upserted = 0
#     new_companies = 0
#     status        = "success"
#     notes         = ""

#     try:
#         conn = db_connect()
#         cur  = conn.cursor()
#         ensure_tables(cur)
#         conn.commit()

#         # Current DB state
#         cur.execute("SELECT COUNT(*) FROM companies;")
#         db_count_before = cur.fetchone()[0]

#         # Resume from the page after what we've already stored.
#         # We go back one page as a safety overlap to catch boundary records.
#         start_page   = max(1, (db_count_before // PAGE_SIZE))
#         empty_streak = 0
#         session      = get_session()

#         log.info(f"DB has {db_count_before} companies. Resuming from page {start_page}.")

#         page = start_page
#         while True:
#             companies = fetch_page(session, page)

#             if companies is None:
#                 # Transient error — skip this page but don't stop
#                 log.warning(f"Skipping page {page} due to fetch error.")
#                 page += 1
#                 time.sleep(REQUEST_DELAY)
#                 continue

#             if not companies:
#                 empty_streak += 1
#                 log.info(f"Empty page {page} ({empty_streak}/{MAX_EMPTY_PAGES})")
#                 if empty_streak >= MAX_EMPTY_PAGES:
#                     log.info("Reached end of available data.")
#                     break
#                 page += 1
#                 time.sleep(REQUEST_DELAY)
#                 continue

#             empty_streak = 0   # reset on successful page
#             synced_at    = datetime.now()
#             rows         = [
#                 to_row(c, synced_at)
#                 for c in companies
#                 if (c.get("co_code") or c.get("CoCode"))
#             ]

#             if rows:
#                 execute_values(cur, SQL_UPSERT, rows)
#                 conn.commit()
#                 rows_upserted += len(rows)
#                 pages_fetched += 1

#                 cur.execute("SELECT COUNT(*) FROM companies;")
#                 db_count_now = cur.fetchone()[0]
#                 newly_added  = db_count_now - db_count_before
#                 new_companies = newly_added

#                 log.info(
#                     f"Page {page:>5} | +{len(rows)} rows | "
#                     f"DB total: {db_count_now} | New: {newly_added}"
#                 )

#             page += 1
#             time.sleep(REQUEST_DELAY)

#         duration = round(time.time() - run_start, 2)
#         log_run(cur, pages_fetched, rows_upserted, new_companies, duration, status)
#         conn.commit()

#         log.info(
#             f"Sync done. Pages: {pages_fetched} | "
#             f"Upserted: {rows_upserted} | New: {new_companies} | "
#             f"Time: {duration}s"
#         )

#     except Exception as e:
#         duration = round(time.time() - run_start, 2)
#         status   = "error"
#         notes    = str(e)
#         log.error(f"Sync failed: {e}")
#         try:
#             log_run(cur, pages_fetched, rows_upserted, new_companies, duration, status, notes)
#             conn.commit()
#         except Exception:
#             pass

#     finally:
#         try:
#             cur.close()
#             conn.close()
#         except Exception:
#             pass

# # ─── SCHEDULER ───────────────────────────────────────────────────────────────

# def build_scheduler():
#     scheduler = BlockingScheduler(timezone=TIMEZONE)

#     if SCHEDULE_MODE == "twice":
#         scheduler.add_job(
#             sync_companies,
#             CronTrigger(hour=SCHEDULE_HOUR_1, minute=0, timezone=TIMEZONE),
#             id="sync_morning",
#             name=f"Morning sync at {SCHEDULE_HOUR_1:02d}:00",
#             misfire_grace_time=300,
#         )
#         scheduler.add_job(
#             sync_companies,
#             CronTrigger(hour=SCHEDULE_HOUR_2, minute=0, timezone=TIMEZONE),
#             id="sync_evening",
#             name=f"Evening sync at {SCHEDULE_HOUR_2:02d}:00",
#             misfire_grace_time=300,
#         )
#         log.info(f"Scheduler: twice daily at {SCHEDULE_HOUR_1:02d}:00 and {SCHEDULE_HOUR_2:02d}:00 {TIMEZONE}")
#     else:
#         scheduler.add_job(
#             sync_companies,
#             CronTrigger(hour=SCHEDULE_HOUR_1, minute=0, timezone=TIMEZONE),
#             id="sync_daily",
#             name=f"Daily sync at {SCHEDULE_HOUR_1:02d}:00",
#             misfire_grace_time=300,
#         )
#         log.info(f"Scheduler: once daily at {SCHEDULE_HOUR_1:02d}:00 {TIMEZONE}")

#     return scheduler

# def handle_shutdown(sig, frame):
#     log.info("Shutdown signal received. Stopping scheduler...")
#     sys.exit(0)

# # ─── ENTRY POINT ─────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     signal.signal(signal.SIGINT,  handle_shutdown)
#     signal.signal(signal.SIGTERM, handle_shutdown)

#     # ── Run once immediately on startup, then hand off to scheduler ──
#     log.info("Running initial sync on startup...")
#     sync_companies()

#     scheduler = build_scheduler()
#     log.info("Scheduler started. Press Ctrl+C to stop.")
#     scheduler.start()




import requests
import psycopg2
import time
import logging
import signal
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from psycopg2.extras import execute_values
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from config.config import CMOTS_TOKEN, DB_CONFIG

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_URL   = "https://equifizapis.cmots.com/api/CompanyMaster"
JWT_TOKEN = CMOTS_TOKEN

# Runs 4 times a day at 6 AM, 12 PM, 6 PM, 12 AM IST
SCHEDULE_HOURS = [0, 6, 12, 18]
TIMEZONE       = "Asia/Kolkata"

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("company_sync.log"),
    ],
)
log = logging.getLogger(__name__)

# ─── SQL ──────────────────────────────────────────────────────────────────────

SQL_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS companies (
    co_code          INTEGER PRIMARY KEY,
    bsecode          VARCHAR(50),
    nsesymbol        VARCHAR(50),
    companyname      VARCHAR(255),
    companyshortname VARCHAR(100),
    categoryname     VARCHAR(100),
    isin             VARCHAR(50),
    bsegroup         VARCHAR(50),
    mcaptype         VARCHAR(50),
    sectorcode       VARCHAR(50),
    sectorname       VARCHAR(100),
    industrycode     VARCHAR(50),
    industryname     VARCHAR(100),
    bselistedflag    VARCHAR(10),
    nselistedflag    VARCHAR(10),
    displaytype      VARCHAR(50),
    synced_at        TIMESTAMP DEFAULT NOW()
);
"""

SQL_CREATE_SYNC_LOG = """
CREATE TABLE IF NOT EXISTS sync_log (
    id            SERIAL PRIMARY KEY,
    run_at        TIMESTAMP DEFAULT NOW(),
    rows_upserted INTEGER,
    new_companies INTEGER,
    duration_secs FLOAT,
    status        VARCHAR(20),
    notes         TEXT
);
"""

SQL_UPSERT = """
INSERT INTO companies (
    co_code, bsecode, nsesymbol, companyname, companyshortname,
    categoryname, isin, bsegroup, mcaptype, sectorcode, sectorname,
    industrycode, industryname, bselistedflag, nselistedflag,
    displaytype, synced_at
) VALUES %s
ON CONFLICT (co_code) DO UPDATE SET
    bsecode          = EXCLUDED.bsecode,
    nsesymbol        = EXCLUDED.nsesymbol,
    companyname      = EXCLUDED.companyname,
    companyshortname = EXCLUDED.companyshortname,
    categoryname     = EXCLUDED.categoryname,
    isin             = EXCLUDED.isin,
    bsegroup         = EXCLUDED.bsegroup,
    mcaptype         = EXCLUDED.mcaptype,
    sectorcode       = EXCLUDED.sectorcode,
    sectorname       = EXCLUDED.sectorname,
    industrycode     = EXCLUDED.industrycode,
    industryname     = EXCLUDED.industryname,
    bselistedflag    = EXCLUDED.bselistedflag,
    nselistedflag    = EXCLUDED.nselistedflag,
    displaytype      = EXCLUDED.displaytype,
    synced_at        = EXCLUDED.synced_at;
"""

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

def fetch_all_companies(session):
    """Single API call — the endpoint returns everything at once."""
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    resp = session.get(API_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    for key in ("data", "Data", "result", "Result"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []

def to_row(c, synced_at):
    def g(*keys):
        for k in keys:
            v = c.get(k)
            if v is not None:
                return v
        return ""
    return (
        g("co_code", "CoCode"),
        g("bsecode", "BseCode"),
        g("nsesymbol", "NseSymbol"),
        g("companyname", "CompanyName"),
        g("companyshortname", "CompanyShortName"),
        g("categoryname", "CategoryName"),
        g("isin", "Isin"),
        g("bsegroup", "BseGroup"),
        g("mcaptype", "McapType"),
        g("sectorcode", "SectorCode"),
        g("sectorname", "SectorName"),
        g("industrycode", "IndustryCode"),
        g("industryname", "IndustryName"),
        g("bselistedflag", "BseListedFlag"),
        g("nselistedflag", "NseListedFlag"),
        g("displaytype", "DisplayType"),
        synced_at,
    )

# ─── CORE SYNC JOB ────────────────────────────────────────────────────────────

def sync_companies():
    run_start = time.time()
    log.info("=" * 60)
    log.info("Starting company sync")

    rows_upserted = 0
    new_companies = 0
    status        = "success"
    notes         = ""
    conn, cur     = None, None

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        cur.execute(SQL_CREATE_TABLE)
        cur.execute(SQL_CREATE_SYNC_LOG)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM companies;")
        count_before = cur.fetchone()[0]

        log.info("Fetching all companies from API...")
        session   = get_session()
        companies = fetch_all_companies(session)
        log.info(f"API returned {len(companies)} records.")

        if not companies:
            notes = "API returned empty list"
            log.warning(notes)
        else:
            synced_at = datetime.now()
            rows = [
                to_row(c, synced_at)
                for c in companies
                if (c.get("co_code") or c.get("CoCode"))
            ]

            if rows:
                execute_values(cur, SQL_UPSERT, rows)
                conn.commit()
                rows_upserted = len(rows)

                cur.execute("SELECT COUNT(*) FROM companies;")
                count_after   = cur.fetchone()[0]
                new_companies = count_after - count_before

                log.info(
                    f"Upserted {rows_upserted} rows | "
                    f"DB total: {count_after} | New: {new_companies}"
                )

        duration = round(time.time() - run_start, 2)
        cur.execute(
            """INSERT INTO sync_log (rows_upserted, new_companies, duration_secs, status, notes)
               VALUES (%s, %s, %s, %s, %s)""",
            (rows_upserted, new_companies, duration, status, notes),
        )
        conn.commit()
        log.info(f"Sync complete in {duration}s")

    except Exception as e:
        duration = round(time.time() - run_start, 2)
        log.error(f"Sync failed: {e}")
        try:
            cur.execute(
                """INSERT INTO sync_log (rows_upserted, new_companies, duration_secs, status, notes)
                   VALUES (%s, %s, %s, %s, %s)""",
                (rows_upserted, new_companies, duration, "error", str(e)),
            )
            conn.commit()
        except Exception:
            pass

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# ─── SCHEDULER ────────────────────────────────────────────────────────────────

def handle_shutdown(sig, frame):
    log.info("Shutdown signal received. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    log.info("Running initial sync on startup...")
    sync_companies()

    scheduler = BlockingScheduler(timezone=TIMEZONE)
    hours = ",".join(str(h) for h in SCHEDULE_HOURS)

    scheduler.add_job(
        sync_companies,
        CronTrigger(hour=hours, minute=0, timezone=TIMEZONE),
        id="sync_4x_daily",
        name="Company sync 4x daily",
        misfire_grace_time=300,
    )

    log.info(f"Scheduler running — syncs at {SCHEDULE_HOURS} (hours) {TIMEZONE}")
    log.info("Press Ctrl+C to stop.")
    scheduler.start()