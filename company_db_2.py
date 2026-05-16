# import requests
# import psycopg2
# import time
# import logging
# import signal
# import sys
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from psycopg2.extras import execute_values
# from datetime import datetime
# from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.triggers.cron import CronTrigger
# import os
# from dotenv import load_dotenv

# load_dotenv()

# # ─── CONFIG ───────────────────────────────────────────────────────────────────

# BASE_URL  = "https://equifizapis.cmots.com/api"
# EQUIFIZ_TOKEN= os.getenv("EQUIFIZ_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I")
# TIMEZONE  = "Asia/Kolkata"
# DB_CONFIG = {
#     "host":     os.getenv("POSTGRES_HOST",     "localhost"),
#     "port":     int(os.getenv("POSTGRES_PORT", "5432")),
#     "dbname":   os.getenv("POSTGRES_DB",       "equifiz"),
#     "user":     os.getenv("POSTGRES_USER",     "postgres"),
#     "password": os.getenv("POSTGRES_PASSWORD", "1234"),
# }

# # ─── LOGGING ──────────────────────────────────────────────────────────────────

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     handlers=[
#         logging.StreamHandler(sys.stdout),
#         logging.FileHandler("master_sync.log"),
#     ],
# )
# log = logging.getLogger(__name__)

# # ─── SQL SETUP ────────────────────────────────────────────────────────────────

# SQL_SETUP = """
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

# CREATE TABLE IF NOT EXISTS group_master (
#     indexcode  INTEGER PRIMARY KEY,
#     exchange   VARCHAR(50),
#     group_name VARCHAR(50),
#     synced_at  TIMESTAMP DEFAULT NOW()
# );

# CREATE TABLE IF NOT EXISTS fund_house (
#     mf_cocode   INTEGER PRIMARY KEY,
#     lname       VARCHAR(100),
#     fund_type   VARCHAR(200),
#     nameamc     VARCHAR(500),
#     address     VARCHAR(1000),
#     telephone   VARCHAR(50),
#     website     VARCHAR(100),
#     email       VARCHAR(100),
#     osch        INTEGER,
#     csch        INTEGER,
#     isch        INTEGER,
#     started_on  TIMESTAMP,
#     sumoftotnav FLOAT,
#     dateas      TIMESTAMP,
#     synced_at   TIMESTAMP DEFAULT NOW()
# );

# CREATE TABLE IF NOT EXISTS scheme_master (
#     mf_cocode             INTEGER,
#     amficode              INTEGER,
#     mf_schcode            INTEGER PRIMARY KEY,
#     classcode             INTEGER,
#     category              VARCHAR(100),
#     sch_name              VARCHAR(500),
#     navrs                 FLOAT,
#     navdate               TIMESTAMP,
#     rtcode                INTEGER,
#     isin                  VARCHAR(50),
#     isin_reinvestment     VARCHAR(50),
#     fundmanager           VARCHAR(500),
#     launchdate            TIMESTAMP,
#     mininvestment         FLOAT,
#     incrementalinvestment FLOAT,
#     mininvestment_sip     FLOAT,
#     frequency             VARCHAR(100),
#     schemeaum             FLOAT,
#     entrytload            VARCHAR(255),
#     exitload              TEXT,
#     fundtype              VARCHAR(100),
#     investmenttype        VARCHAR(100),
#     mcapcategory          VARCHAR(100),
#     bmcode                INTEGER,
#     benchmarkname         VARCHAR(255),
#     riskometervalue       VARCHAR(100),
#     schemeinvestmenttype  VARCHAR(100),
#     schemetype            VARCHAR(100),
#     groupcode             VARCHAR(50),
#     groupname             VARCHAR(255),
#     maturitydate          VARCHAR(100),
#     lockinperiod          VARCHAR(100),
#     inceptiondate         TIMESTAMP,
#     synced_at             TIMESTAMP DEFAULT NOW()
# );

# CREATE TABLE IF NOT EXISTS etf_master (
#     equity_cmotscode INTEGER PRIMARY KEY,   -- Equity CMOTS Code (unique ETF identifier)
#     mf_cmotscode     INTEGER,               -- MF CMOTS Code (links to fund_house.mf_cocode)
#     amcname          VARCHAR(50),           -- AMC Name
#     etfname          VARCHAR(200),          -- ETF Name
#     isin             VARCHAR(50),           -- ISIN
#     etfcategory      VARCHAR(50),           -- ETF Category (e.g. Equity, Debt, Gold, etc.)
#     bselisted        INTEGER,               -- BSE Listed flag (1 = yes, 0 = no)
#     nselisted        INTEGER,               -- NSE Listed flag (1 = yes, 0 = no)
#     synced_at        TIMESTAMP DEFAULT NOW()
# );
# """

# # ─── HELPERS ──────────────────────────────────────────────────────────────────

# def get_session():
#     session = requests.Session()
#     retry   = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
#     session.mount("https://", HTTPAdapter(max_retries=retry))
#     return session


# def fetch(session, url):
#     headers = {"Authorization": f"Bearer {EQUIFIZ_TOKEN}"}
#     resp    = session.get(url, headers=headers, timeout=60)
#     resp.raise_for_status()
#     data = resp.json()
#     if isinstance(data, list):
#         return data
#     for key in ("data", "Data", "result", "Result"):
#         if key in data and isinstance(data[key], list):
#             return data[key]
#     return []


# def g(record, *keys):
#     """Return first non-None value from record by trying multiple key casings."""
#     for k in keys:
#         v = record.get(k)
#         if v is not None:
#             return v
#     return ""


# def to_int(val):
#     try:
#         return int(val) if val not in (None, "", "N/A") else None
#     except (ValueError, TypeError):
#         return None


# def to_float(val):
#     try:
#         return float(val) if val not in (None, "", "N/A") else None
#     except (ValueError, TypeError):
#         return None


# def to_str(val):
#     if val in (None, ""):
#         return None
#     return str(val)


# # ─── JOB: COMPANY MASTER ──────────────────────────────────────────────────────

# def sync_companies(conn):
#     job       = "company_master"
#     run_start = time.time()
#     log.info(f"[{job}] Starting sync...")

#     cur = conn.cursor()
#     cur.execute("SELECT COUNT(*) FROM companies;")
#     count_before = cur.fetchone()[0]

#     records = fetch(get_session(), f"{BASE_URL}/CompanyMaster")
#     log.info(f"[{job}] API returned {len(records)} records.")

#     if not records:
#         log.warning(f"[{job}] API returned empty list")
#         return

#     synced_at = datetime.now()
#     rows      = []
#     for c in records:
#         co_code = c.get("co_code") or c.get("CoCode")
#         if not co_code:
#             continue
#         rows.append((
#             co_code,
#             g(c, "bsecode",          "BseCode"),
#             g(c, "nsesymbol",        "NseSymbol"),
#             g(c, "companyname",      "CompanyName"),
#             g(c, "companyshortname", "CompanyShortName"),
#             g(c, "categoryname",     "CategoryName"),
#             g(c, "isin",             "Isin"),
#             g(c, "bsegroup",         "BseGroup"),
#             g(c, "mcaptype",         "McapType"),
#             g(c, "sectorcode",       "SectorCode"),
#             g(c, "sectorname",       "SectorName"),
#             g(c, "industrycode",     "IndustryCode"),
#             g(c, "industryname",     "IndustryName"),
#             g(c, "bselistedflag",    "BseListedFlag"),
#             g(c, "nselistedflag",    "NseListedFlag"),
#             g(c, "displaytype",      "DisplayType"),
#             synced_at,
#         ))

#     execute_values(cur, """
#         INSERT INTO companies (
#             co_code, bsecode, nsesymbol, companyname, companyshortname,
#             categoryname, isin, bsegroup, mcaptype, sectorcode, sectorname,
#             industrycode, industryname, bselistedflag, nselistedflag,
#             displaytype, synced_at
#         ) VALUES %s
#         ON CONFLICT (co_code) DO UPDATE SET
#             bsecode          = EXCLUDED.bsecode,
#             nsesymbol        = EXCLUDED.nsesymbol,
#             companyname      = EXCLUDED.companyname,
#             companyshortname = EXCLUDED.companyshortname,
#             categoryname     = EXCLUDED.categoryname,
#             isin             = EXCLUDED.isin,
#             bsegroup         = EXCLUDED.bsegroup,
#             mcaptype         = EXCLUDED.mcaptype,
#             sectorcode       = EXCLUDED.sectorcode,
#             sectorname       = EXCLUDED.sectorname,
#             industrycode     = EXCLUDED.industrycode,
#             industryname     = EXCLUDED.industryname,
#             bselistedflag    = EXCLUDED.bselistedflag,
#             nselistedflag    = EXCLUDED.nselistedflag,
#             displaytype      = EXCLUDED.displaytype,
#             synced_at        = EXCLUDED.synced_at
#     """, rows)

#     cur.execute("SELECT COUNT(*) FROM companies;")
#     new_rows = cur.fetchone()[0] - count_before
#     duration = round(time.time() - run_start, 2)
#     conn.commit()
#     log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# # ─── JOB: GROUP MASTER ────────────────────────────────────────────────────────

# def sync_group_master(conn):
#     job       = "group_master"
#     run_start = time.time()
#     log.info(f"[{job}] Starting sync...")

#     cur = conn.cursor()
#     cur.execute("SELECT COUNT(*) FROM group_master;")
#     count_before = cur.fetchone()[0]

#     session = get_session()
#     records = []
#     for exchange in ("BSE", "NSE"):
#         data = fetch(session, f"{BASE_URL}/GroupMaster/{exchange}")
#         log.info(f"[{job}] {exchange} returned {len(data)} records.")
#         records.extend(data)

#     if not records:
#         log.warning(f"[{job}] API returned empty list")
#         return

#     synced_at = datetime.now()
#     rows = [
#         (
#             r.get("indexcode") or r.get("IndexCode"),
#             r.get("exchange")  or r.get("Exchange"),
#             r.get("group")     or r.get("Group"),
#             synced_at,
#         )
#         for r in records
#         if (r.get("indexcode") or r.get("IndexCode")) is not None
#     ]

#     execute_values(cur, """
#         INSERT INTO group_master (indexcode, exchange, group_name, synced_at)
#         VALUES %s
#         ON CONFLICT (indexcode) DO UPDATE SET
#             exchange   = EXCLUDED.exchange,
#             group_name = EXCLUDED.group_name,
#             synced_at  = EXCLUDED.synced_at
#     """, rows)

#     cur.execute("SELECT COUNT(*) FROM group_master;")
#     new_rows = cur.fetchone()[0] - count_before
#     duration = round(time.time() - run_start, 2)
#     conn.commit()
#     log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# # ─── JOB: FUND HOUSE ──────────────────────────────────────────────────────────

# def sync_fund_house(conn):
#     job       = "fund_house"
#     run_start = time.time()
#     log.info(f"[{job}] Starting sync...")

#     cur = conn.cursor()
#     cur.execute("SELECT COUNT(*) FROM fund_house;")
#     count_before = cur.fetchone()[0]

#     records = fetch(get_session(), f"{BASE_URL}/Fund_House")
#     log.info(f"[{job}] API returned {len(records)} records.")

#     if not records:
#         log.warning(f"[{job}] API returned empty list")
#         return

#     synced_at = datetime.now()
#     rows      = []
#     for r in records:
#         mf_cocode = r.get("mf_cocode") or r.get("MfCoCode")
#         if not mf_cocode:
#             continue
#         rows.append((
#             mf_cocode,
#             g(r, "lname",       "LName"),
#             g(r, "fund_type",   "FundType"),
#             g(r, "nameamc",     "NameAMC"),
#             g(r, "address",     "Address"),
#             g(r, "telephone",   "Telephone")   or None,
#             g(r, "website",     "Website"),
#             g(r, "email",       "Email"),
#             g(r, "osch",        "Osch")        or None,
#             g(r, "csch",        "Csch")        or None,
#             g(r, "isch",        "Isch")        or None,
#             g(r, "started_on",  "StartedOn")   or None,
#             g(r, "sumoftotnav", "SumOfTotNav") or None,
#             g(r, "dateas",      "DateAs")      or None,
#             synced_at,
#         ))

#     execute_values(cur, """
#         INSERT INTO fund_house (
#             mf_cocode, lname, fund_type, nameamc, address, telephone,
#             website, email, osch, csch, isch, started_on, sumoftotnav,
#             dateas, synced_at
#         ) VALUES %s
#         ON CONFLICT (mf_cocode) DO UPDATE SET
#             lname       = EXCLUDED.lname,
#             fund_type   = EXCLUDED.fund_type,
#             nameamc     = EXCLUDED.nameamc,
#             address     = EXCLUDED.address,
#             telephone   = EXCLUDED.telephone,
#             website     = EXCLUDED.website,
#             email       = EXCLUDED.email,
#             osch        = EXCLUDED.osch,
#             csch        = EXCLUDED.csch,
#             isch        = EXCLUDED.isch,
#             started_on  = EXCLUDED.started_on,
#             sumoftotnav = EXCLUDED.sumoftotnav,
#             dateas      = EXCLUDED.dateas,
#             synced_at   = EXCLUDED.synced_at
#     """, rows)

#     cur.execute("SELECT COUNT(*) FROM fund_house;")
#     new_rows = cur.fetchone()[0] - count_before
#     duration = round(time.time() - run_start, 2)
#     conn.commit()
#     log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# # ─── JOB: SCHEME MASTER ───────────────────────────────────────────────────────

# def sync_scheme_master(conn):
#     job       = "scheme_master"
#     run_start = time.time()
#     log.info(f"[{job}] Starting sync...")

#     cur = conn.cursor()
#     cur.execute("SELECT COUNT(*) FROM scheme_master;")
#     count_before = cur.fetchone()[0]

#     cur.execute("SELECT mf_cocode FROM fund_house;")
#     mf_cocodes = [row[0] for row in cur.fetchall()]

#     if not mf_cocodes:
#         log.warning(f"[{job}] No fund houses found — skipping scheme master sync")
#         return

#     session   = get_session()
#     synced_at = datetime.now()
#     all_rows  = []

#     for mf_cocode in mf_cocodes:
#         try:
#             records = fetch(session, f"{BASE_URL}/SchemeMaster/{mf_cocode}")
#             if not records:
#                 continue
#             log.info(f"[{job}] mf_cocode={mf_cocode} → {len(records)} records")
#             for r in records:
#                 mf_schcode = to_int(r.get("mf_schcode") or r.get("MfSchCode"))
#                 if not mf_schcode:
#                     continue
#                 all_rows.append((
#                     to_int(r.get("mf_cocode")  or r.get("MfCoCode")  or mf_cocode),
#                     to_int(g(r, "amficode",     "AmfiCode")),
#                     mf_schcode,
#                     to_int(g(r, "classcode",    "ClassCode")),
#                     to_str(g(r, "category",     "Category")),
#                     to_str(g(r, "sch_name",     "SchName")),
#                     to_float(g(r, "navrs",      "Navrs")),
#                     to_str(g(r, "navdate",      "NavDate"))   or None,
#                     to_int(g(r, "rtcode",       "RtCode")),
#                     to_str(g(r, "isin",         "Isin")),
#                     to_str(g(r, "isin_reinvestment", "IsinReinvestment")),
#                     to_str(g(r, "fundmanager",  "FundManager")),
#                     to_str(g(r, "launchdate",   "LaunchDate")) or None,
#                     to_float(g(r, "mininvestment",         "MinInvestment")),
#                     to_float(g(r, "incrementalinvestment", "IncrementalInvestment")),
#                     to_float(g(r, "mininvestment_sip",     "MinInvestmentSip")),
#                     to_str(g(r, "frequency",    "Frequency")),
#                     to_float(g(r, "schemeaum",  "SchemeAum")),
#                     to_str(g(r, "entrytload",   "EntrytLoad")),
#                     to_str(g(r, "exitload",     "ExitLoad")),
#                     to_str(g(r, "fundtype",     "FundType")),
#                     to_str(g(r, "investmenttype", "InvestmentType")),
#                     to_str(g(r, "mcapcategory", "McapCategory")),
#                     to_int(g(r, "bmcode",       "BmCode")),
#                     to_str(g(r, "benchmarkname","BenchmarkName")),
#                     to_str(g(r, "riskometervalue", "RiskometerValue")),
#                     to_str(g(r, "schemeinvestmenttype", "SchemeInvestmentType")),
#                     to_str(g(r, "schemetype",   "SchemeType")),
#                     to_str(g(r, "groupcode",    "GroupCode")),
#                     to_str(g(r, "groupname",    "GroupName")),
#                     to_str(g(r, "maturitydate", "MaturityDate")),
#                     to_str(g(r, "lockinperiod", "LockInPeriod")),
#                     to_str(g(r, "inceptiondate","InceptionDate")) or None,
#                     synced_at,
#                 ))
#         except Exception as e:
#             log.warning(f"[{job}] mf_cocode={mf_cocode} fetch failed: {e}")
#             continue

#     if not all_rows:
#         log.warning(f"[{job}] No scheme records collected")
#         return

#     execute_values(cur, """
#         INSERT INTO scheme_master (
#             mf_cocode, amficode, mf_schcode, classcode, category, sch_name,
#             navrs, navdate, rtcode, isin, isin_reinvestment, fundmanager,
#             launchdate, mininvestment, incrementalinvestment, mininvestment_sip,
#             frequency, schemeaum, entrytload, exitload, fundtype, investmenttype,
#             mcapcategory, bmcode, benchmarkname, riskometervalue,
#             schemeinvestmenttype, schemetype, groupcode, groupname,
#             maturitydate, lockinperiod, inceptiondate, synced_at
#         ) VALUES %s
#         ON CONFLICT (mf_schcode) DO UPDATE SET
#             mf_cocode             = EXCLUDED.mf_cocode,
#             amficode              = EXCLUDED.amficode,
#             classcode             = EXCLUDED.classcode,
#             category              = EXCLUDED.category,
#             sch_name              = EXCLUDED.sch_name,
#             navrs                 = EXCLUDED.navrs,
#             navdate               = EXCLUDED.navdate,
#             rtcode                = EXCLUDED.rtcode,
#             isin                  = EXCLUDED.isin,
#             isin_reinvestment     = EXCLUDED.isin_reinvestment,
#             fundmanager           = EXCLUDED.fundmanager,
#             launchdate            = EXCLUDED.launchdate,
#             mininvestment         = EXCLUDED.mininvestment,
#             incrementalinvestment = EXCLUDED.incrementalinvestment,
#             mininvestment_sip     = EXCLUDED.mininvestment_sip,
#             frequency             = EXCLUDED.frequency,
#             schemeaum             = EXCLUDED.schemeaum,
#             entrytload            = EXCLUDED.entrytload,
#             exitload              = EXCLUDED.exitload,
#             fundtype              = EXCLUDED.fundtype,
#             investmenttype        = EXCLUDED.investmenttype,
#             mcapcategory          = EXCLUDED.mcapcategory,
#             bmcode                = EXCLUDED.bmcode,
#             benchmarkname         = EXCLUDED.benchmarkname,
#             riskometervalue       = EXCLUDED.riskometervalue,
#             schemeinvestmenttype  = EXCLUDED.schemeinvestmenttype,
#             schemetype            = EXCLUDED.schemetype,
#             groupcode             = EXCLUDED.groupcode,
#             groupname             = EXCLUDED.groupname,
#             maturitydate          = EXCLUDED.maturitydate,
#             lockinperiod          = EXCLUDED.lockinperiod,
#             inceptiondate         = EXCLUDED.inceptiondate,
#             synced_at             = EXCLUDED.synced_at
#     """, all_rows)

#     cur.execute("SELECT COUNT(*) FROM scheme_master;")
#     new_rows = cur.fetchone()[0] - count_before
#     duration = round(time.time() - run_start, 2)
#     conn.commit()
#     log.info(f"[{job}] Done — {len(all_rows)} upserted, {new_rows} new in {duration}s")


# # ─── JOB: ETF MASTER ──────────────────────────────────────────────────────────
# # Source : GET /api/ETFMaster
# # Schedule: EOD, once on trading day (11:30 PM – 11:55 PM IST)
# # Primary key: equity_cmotscode

# def sync_etf_master(conn):
#     job       = "etf_master"
#     run_start = time.time()
#     log.info(f"[{job}] Starting sync...")

#     cur = conn.cursor()
#     cur.execute("SELECT COUNT(*) FROM etf_master;")
#     count_before = cur.fetchone()[0]

#     records = fetch(get_session(), f"{BASE_URL}/ETFMaster")
#     log.info(f"[{job}] API returned {len(records)} records.")

#     if not records:
#         log.warning(f"[{job}] API returned empty list")
#         return

#     synced_at = datetime.now()
#     rows      = []
#     for r in records:
#         equity_cmotscode = to_int(
#             r.get("equity_cmotscode") or r.get("EquityCmotsCode") or r.get("Equity_CMOTSCode")
#         )
#         if not equity_cmotscode:
#             log.debug(f"[{job}] Skipping record with no equity_cmotscode: {r}")
#             continue
#         rows.append((
#             equity_cmotscode,
#             to_int(g(r,  "mf_cmotscode",  "MF_CMOTSCode",   "MfCmotsCode")),
#             to_str(g(r,  "amcname",       "AMCName",         "AmcName")),
#             to_str(g(r,  "etfname",       "ETFName",         "EtfName")),
#             to_str(g(r,  "isin",          "ISIN",            "Isin")),
#             to_str(g(r,  "etfcategory",   "ETFCategory",     "EtfCategory")),
#             to_int(g(r,  "bselisted",     "BSEListed",       "BseListed")),
#             to_int(g(r,  "nselisted",     "NSEListed",       "NseListed")),
#             synced_at,
#         ))

#     if not rows:
#         log.warning(f"[{job}] No valid rows to upsert")
#         return

#     execute_values(cur, """
#         INSERT INTO etf_master (
#             equity_cmotscode, mf_cmotscode, amcname, etfname,
#             isin, etfcategory, bselisted, nselisted, synced_at
#         ) VALUES %s
#         ON CONFLICT (equity_cmotscode) DO UPDATE SET
#             mf_cmotscode = EXCLUDED.mf_cmotscode,
#             amcname      = EXCLUDED.amcname,
#             etfname      = EXCLUDED.etfname,
#             isin         = EXCLUDED.isin,
#             etfcategory  = EXCLUDED.etfcategory,
#             bselisted    = EXCLUDED.bselisted,
#             nselisted    = EXCLUDED.nselisted,
#             synced_at    = EXCLUDED.synced_at
#     """, rows)

#     cur.execute("SELECT COUNT(*) FROM etf_master;")
#     new_rows = cur.fetchone()[0] - count_before
#     duration = round(time.time() - run_start, 2)
#     conn.commit()
#     log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# # ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

# JOBS = [
#     sync_companies,
#     sync_group_master,
#     sync_fund_house,
#     sync_scheme_master,
#     sync_etf_master,        # ← new
# ]


# def run_single(job_fn):
#     """Run a single job with its own DB connection."""
#     conn = None
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)
#         job_fn(conn)
#     except Exception as e:
#         log.error(f"[{job_fn.__name__}] Failed: {e}")
#     finally:
#         if conn:
#             conn.close()


# def run_all():
#     log.info("=" * 60)
#     log.info("Starting full sync run")
#     total_start = time.time()

#     conn = None
#     try:
#         conn = psycopg2.connect(**DB_CONFIG)

#         cur = conn.cursor()
#         cur.execute(SQL_SETUP)
#         conn.commit()
#         cur.close()

#         for job_fn in JOBS:
#             try:
#                 job_fn(conn)
#             except Exception as e:
#                 log.error(f"[{job_fn.__name__}] Failed: {e}")
#     finally:
#         if conn:
#             conn.close()

#     log.info(f"Full sync run complete in {round(time.time() - total_start, 2)}s")
#     log.info("=" * 60)


# # ─── SCHEDULER ────────────────────────────────────────────────────────────────

# def handle_shutdown(sig, frame):
#     log.info("Shutdown signal received. Exiting.")
#     sys.exit(0)


# if __name__ == "__main__":
#     signal.signal(signal.SIGINT,  handle_shutdown)
#     signal.signal(signal.SIGTERM, handle_shutdown)

#     log.info("Running initial sync on startup...")
#     run_all()

#     scheduler = BlockingScheduler(timezone=TIMEZONE)

#     # Company master: 4x daily
#     scheduler.add_job(
#         lambda: run_single(sync_companies),
#         CronTrigger(hour="0,6,12,18", minute=0, timezone=TIMEZONE),
#         id="company_master",
#         name="Company master 4x daily",
#         misfire_grace_time=300,
#     )

#     # Group master: once daily at 11:30 PM
#     scheduler.add_job(
#         lambda: run_single(sync_group_master),
#         CronTrigger(hour=23, minute=30, timezone=TIMEZONE),
#         id="group_master",
#         name="Group master daily",
#         misfire_grace_time=300,
#     )

#     # Fund house: once daily at 11:00 PM
#     scheduler.add_job(
#         lambda: run_single(sync_fund_house),
#         CronTrigger(hour=23, minute=0, timezone=TIMEZONE),
#         id="fund_house",
#         name="Fund house daily",
#         misfire_grace_time=300,
#     )

#     # Scheme master: once daily at 11:05 PM (after fund_house)
#     scheduler.add_job(
#         lambda: run_single(sync_scheme_master),
#         CronTrigger(hour=23, minute=5, timezone=TIMEZONE),
#         id="scheme_master",
#         name="Scheme master daily",
#         misfire_grace_time=300,
#     )

#     # ETF master: once daily at 11:35 PM (EOD, 11:30–11:55 PM window per API docs)
#     scheduler.add_job(
#         lambda: run_single(sync_etf_master),
#         CronTrigger(hour=23, minute=35, timezone=TIMEZONE),
#         id="etf_master",
#         name="ETF master daily",
#         misfire_grace_time=300,
#     )

#     log.info("Scheduler running. Press Ctrl+C to stop.")
#     scheduler.start()














import os
import sys
import time
import signal
import logging

import requests
import psycopg2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from psycopg2.extras import execute_values
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_URL      = "https://equifizapis.cmots.com/api"
EQUIFIZ_TOKEN = os.getenv("EQUIFIZ_TOKEN", "")
TIMEZONE      = "Asia/Kolkata"

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "equifiz"),
    "user":     os.getenv("POSTGRES_USER",     "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "1234"),
}

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("master_sync.log"),
    ],
)
log = logging.getLogger(__name__)

# ─── SQL SETUP ────────────────────────────────────────────────────────────────

SQL_SETUP = """
CREATE TABLE IF NOT EXISTS companies (
    co_code          INTEGER      PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS group_master (
    indexcode  INTEGER     PRIMARY KEY,
    exchange   VARCHAR(50),
    group_name VARCHAR(50),
    synced_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fund_house (
    mf_cocode   INTEGER       PRIMARY KEY,
    lname       VARCHAR(100),
    fund_type   VARCHAR(200),
    nameamc     VARCHAR(500),
    address     VARCHAR(1000),
    telephone   VARCHAR(50),
    website     VARCHAR(100),
    email       VARCHAR(100),
    osch        INTEGER,
    csch        INTEGER,
    isch        INTEGER,
    started_on  TIMESTAMP,
    sumoftotnav FLOAT,
    dateas      TIMESTAMP,
    synced_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheme_master (
    mf_cocode             INTEGER,
    amficode              INTEGER,
    mf_schcode            INTEGER      PRIMARY KEY,
    classcode             INTEGER,
    category              VARCHAR(100),
    sch_name              VARCHAR(500),
    navrs                 FLOAT,
    navdate               TIMESTAMP,
    rtcode                INTEGER,
    isin                  VARCHAR(50),
    isin_reinvestment     VARCHAR(50),
    fundmanager           VARCHAR(500),
    launchdate            TIMESTAMP,
    mininvestment         FLOAT,
    incrementalinvestment FLOAT,
    mininvestment_sip     FLOAT,
    frequency             VARCHAR(100),
    schemeaum             FLOAT,
    entrytload            VARCHAR(255),
    exitload              TEXT,
    fundtype              VARCHAR(100),
    investmenttype        VARCHAR(100),
    mcapcategory          VARCHAR(100),
    bmcode                INTEGER,
    benchmarkname         VARCHAR(255),
    riskometervalue       VARCHAR(100),
    schemeinvestmenttype  VARCHAR(100),
    schemetype            VARCHAR(100),
    groupcode             VARCHAR(50),
    groupname             VARCHAR(255),
    maturitydate          VARCHAR(100),
    lockinperiod          VARCHAR(100),
    inceptiondate         TIMESTAMP,
    synced_at             TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etf_master (
    equity_cmotscode INTEGER      PRIMARY KEY,
    mf_cmotscode     INTEGER,
    amcname          VARCHAR(200),
    etfname          VARCHAR(200),
    isin             VARCHAR(50),
    etfcategory      VARCHAR(200),
    bselisted        INTEGER,
    nselisted        INTEGER,
    synced_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ipo_master (
    co_code          INTEGER      PRIMARY KEY,
    isin             INTEGER,
    companyshortname VARCHAR(200),
    company_name     VARCHAR(255),
    issue            VARCHAR(200),
    issuetype        VARCHAR(200),
    opendate         TIMESTAMP,
    closedate        TIMESTAMP,
    type             VARCHAR(50),
    ipotype          VARCHAR(200),
    freshissue       INTEGER,
    mininvestment    INTEGER,
    synced_at        TIMESTAMP DEFAULT NOW()
);

-- ── bond_master: all VARCHAR columns are intentionally oversized.
--    The API docs show Varchar(50/100) but real data routinely exceeds those
--    limits (multi-agency credit ratings, long security descriptions, etc.).
--    Use TEXT for any field that is unbounded in practice.
CREATE TABLE IF NOT EXISTS bond_master (
    code           INTEGER       PRIMARY KEY,
    companyname    VARCHAR(500),
    bsegroup       INTEGER,
    bsecode        INTEGER,
    bsescripname   VARCHAR(500),
    bselistdate    TIMESTAMP,
    nsesymbol      VARCHAR(200),
    nseseries      VARCHAR(200),
    nsesecname     VARCHAR(500),
    nselistdate    TIMESTAMP,
    secdesc        TEXT,           -- API says Varchar(500) but content can be much longer
    totsecurities  BIGINT,
    issprice       BIGINT,
    fv             BIGINT,
    paidupval      BIGINT,
    mktlot         BIGINT,
    allotdate      TIMESTAMP,
    redeemdate     TIMESTAMP,
    redeemamt      BIGINT,
    redeemnote     BIGINT,
    isin           VARCHAR(50),
    tenor          TEXT,           -- API says Varchar(50) but real values can exceed this
    cprate         FLOAT,
    intfrequency   VARCHAR(200),
    pcoption       TEXT,           -- API says Varchar(50); real data is much longer
    creditrating   TEXT,           -- API says Varchar(50); multi-agency strings are very long
    bondtype       VARCHAR(200),
    synced_at      TIMESTAMP DEFAULT NOW()
);

-- ── Idempotent migrations for all other tables (bond_master excluded — it is
--    always created fresh with correct types above).
DO $$
BEGIN
    -- ipo_master
    ALTER TABLE ipo_master ALTER COLUMN companyshortname TYPE VARCHAR(200);
    ALTER TABLE ipo_master ALTER COLUMN company_name     TYPE VARCHAR(255);
    ALTER TABLE ipo_master ALTER COLUMN issue            TYPE VARCHAR(200);
    ALTER TABLE ipo_master ALTER COLUMN issuetype        TYPE VARCHAR(200);
    ALTER TABLE ipo_master ALTER COLUMN ipotype          TYPE VARCHAR(200);
    -- etf_master
    ALTER TABLE etf_master ALTER COLUMN amcname     TYPE VARCHAR(200);
    ALTER TABLE etf_master ALTER COLUMN etfcategory TYPE VARCHAR(200);
EXCEPTION WHEN others THEN
    NULL;
END $$;
"""

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_session() -> requests.Session:
    session = requests.Session()
    retry   = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch(session: requests.Session, url: str) -> list:
    """GET *url* and return the list payload, trying common envelope keys."""
    headers = {"Authorization": f"Bearer {EQUIFIZ_TOKEN}"}
    resp    = session.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    for key in ("data", "Data", "result", "Result"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def g(record: dict, *keys):
    """Return the first non-None value found in *record* by trying *keys*."""
    for k in keys:
        v = record.get(k)
        if v is not None:
            return v
    return ""


def to_int(val) -> int | None:
    try:
        return int(val) if val not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None


def to_float(val) -> float | None:
    try:
        return float(val) if val not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None


def to_str(val) -> str | None:
    if val in (None, ""):
        return None
    return str(val)


def to_ts(val) -> str | None:
    """Return val as a string timestamp or None (psycopg2 handles parsing)."""
    if val in (None, "", "N/A"):
        return None
    return str(val)


# ─── JOB: COMPANY MASTER ──────────────────────────────────────────────────────

def sync_companies(conn: psycopg2.extensions.connection) -> None:
    job = "company_master"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM companies;")
    before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/CompanyMaster")
    log.info(f"[{job}] API returned {len(records)} records.")
    if not records:
        log.warning(f"[{job}] Empty response — skipping.")
        return

    synced_at = datetime.now()
    rows = []
    for c in records:
        co_code = c.get("co_code") or c.get("CoCode")
        if not co_code:
            continue
        rows.append((
            co_code,
            g(c, "bsecode",          "BseCode"),
            g(c, "nsesymbol",        "NseSymbol"),
            g(c, "companyname",      "CompanyName"),
            g(c, "companyshortname", "CompanyShortName"),
            g(c, "categoryname",     "CategoryName"),
            g(c, "isin",             "Isin"),
            g(c, "bsegroup",         "BseGroup"),
            g(c, "mcaptype",         "McapType"),
            g(c, "sectorcode",       "SectorCode"),
            g(c, "sectorname",       "SectorName"),
            g(c, "industrycode",     "IndustryCode"),
            g(c, "industryname",     "IndustryName"),
            g(c, "bselistedflag",    "BseListedFlag"),
            g(c, "nselistedflag",    "NseListedFlag"),
            g(c, "displaytype",      "DisplayType"),
            synced_at,
        ))

    execute_values(cur, """
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
            synced_at        = EXCLUDED.synced_at
    """, rows)

    cur.execute("SELECT COUNT(*) FROM companies;")
    new  = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── JOB: GROUP MASTER ────────────────────────────────────────────────────────

def sync_group_master(conn: psycopg2.extensions.connection) -> None:
    job = "group_master"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM group_master;")
    before = cur.fetchone()[0]

    session = get_session()
    records = []
    for exchange in ("BSE", "NSE"):
        data = fetch(session, f"{BASE_URL}/GroupMaster/{exchange}")
        log.info(f"[{job}] {exchange} returned {len(data)} records.")
        records.extend(data)

    if not records:
        log.warning(f"[{job}] Empty response — skipping.")
        return

    synced_at = datetime.now()
    rows = [
        (
            r.get("indexcode") or r.get("IndexCode"),
            r.get("exchange")  or r.get("Exchange"),
            r.get("group")     or r.get("Group"),
            synced_at,
        )
        for r in records
        if (r.get("indexcode") or r.get("IndexCode")) is not None
    ]

    execute_values(cur, """
        INSERT INTO group_master (indexcode, exchange, group_name, synced_at)
        VALUES %s
        ON CONFLICT (indexcode) DO UPDATE SET
            exchange   = EXCLUDED.exchange,
            group_name = EXCLUDED.group_name,
            synced_at  = EXCLUDED.synced_at
    """, rows)

    cur.execute("SELECT COUNT(*) FROM group_master;")
    new = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── JOB: FUND HOUSE ──────────────────────────────────────────────────────────

def sync_fund_house(conn: psycopg2.extensions.connection) -> None:
    job = "fund_house"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fund_house;")
    before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/Fund_House")
    log.info(f"[{job}] API returned {len(records)} records.")
    if not records:
        log.warning(f"[{job}] Empty response — skipping.")
        return

    synced_at = datetime.now()
    rows = []
    for r in records:
        mf_cocode = r.get("mf_cocode") or r.get("MfCoCode")
        if not mf_cocode:
            continue
        rows.append((
            mf_cocode,
            g(r, "lname",       "LName"),
            g(r, "fund_type",   "FundType"),
            g(r, "nameamc",     "NameAMC"),
            g(r, "address",     "Address"),
            to_str(g(r, "telephone",   "Telephone")),
            g(r, "website",     "Website"),
            g(r, "email",       "Email"),
            to_int(g(r, "osch", "Osch")),
            to_int(g(r, "csch", "Csch")),
            to_int(g(r, "isch", "Isch")),
            to_ts(g(r,  "started_on",  "StartedOn")),
            to_float(g(r, "sumoftotnav", "SumOfTotNav")),
            to_ts(g(r,  "dateas",      "DateAs")),
            synced_at,
        ))

    execute_values(cur, """
        INSERT INTO fund_house (
            mf_cocode, lname, fund_type, nameamc, address, telephone,
            website, email, osch, csch, isch, started_on, sumoftotnav,
            dateas, synced_at
        ) VALUES %s
        ON CONFLICT (mf_cocode) DO UPDATE SET
            lname       = EXCLUDED.lname,
            fund_type   = EXCLUDED.fund_type,
            nameamc     = EXCLUDED.nameamc,
            address     = EXCLUDED.address,
            telephone   = EXCLUDED.telephone,
            website     = EXCLUDED.website,
            email       = EXCLUDED.email,
            osch        = EXCLUDED.osch,
            csch        = EXCLUDED.csch,
            isch        = EXCLUDED.isch,
            started_on  = EXCLUDED.started_on,
            sumoftotnav = EXCLUDED.sumoftotnav,
            dateas      = EXCLUDED.dateas,
            synced_at   = EXCLUDED.synced_at
    """, rows)

    cur.execute("SELECT COUNT(*) FROM fund_house;")
    new = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── JOB: SCHEME MASTER ───────────────────────────────────────────────────────

def sync_scheme_master(conn: psycopg2.extensions.connection) -> None:
    job = "scheme_master"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scheme_master;")
    before = cur.fetchone()[0]

    cur.execute("SELECT mf_cocode FROM fund_house;")
    mf_cocodes = [row[0] for row in cur.fetchall()]
    if not mf_cocodes:
        log.warning(f"[{job}] No fund houses found — skipping.")
        return

    session   = get_session()
    synced_at = datetime.now()
    all_rows  = []

    for mf_cocode in mf_cocodes:
        try:
            records = fetch(session, f"{BASE_URL}/SchemeMaster/{mf_cocode}")
            if not records:
                continue
            log.info(f"[{job}] mf_cocode={mf_cocode} → {len(records)} records")
            for r in records:
                mf_schcode = to_int(r.get("mf_schcode") or r.get("MfSchCode"))
                if not mf_schcode:
                    continue
                all_rows.append((
                    to_int(r.get("mf_cocode") or r.get("MfCoCode") or mf_cocode),
                    to_int(g(r,  "amficode",             "AmfiCode")),
                    mf_schcode,
                    to_int(g(r,  "classcode",            "ClassCode")),
                    to_str(g(r,  "category",             "Category")),
                    to_str(g(r,  "sch_name",             "SchName")),
                    to_float(g(r,"navrs",                "Navrs")),
                    to_ts(g(r,   "navdate",              "NavDate")),
                    to_int(g(r,  "rtcode",               "RtCode")),
                    to_str(g(r,  "isin",                 "Isin")),
                    to_str(g(r,  "isin_reinvestment",    "IsinReinvestment")),
                    to_str(g(r,  "fundmanager",          "FundManager")),
                    to_ts(g(r,   "launchdate",           "LaunchDate")),
                    to_float(g(r,"mininvestment",        "MinInvestment")),
                    to_float(g(r,"incrementalinvestment","IncrementalInvestment")),
                    to_float(g(r,"mininvestment_sip",    "MinInvestmentSip")),
                    to_str(g(r,  "frequency",            "Frequency")),
                    to_float(g(r,"schemeaum",            "SchemeAum")),
                    to_str(g(r,  "entrytload",           "EntrytLoad")),
                    to_str(g(r,  "exitload",             "ExitLoad")),
                    to_str(g(r,  "fundtype",             "FundType")),
                    to_str(g(r,  "investmenttype",       "InvestmentType")),
                    to_str(g(r,  "mcapcategory",         "McapCategory")),
                    to_int(g(r,  "bmcode",               "BmCode")),
                    to_str(g(r,  "benchmarkname",        "BenchmarkName")),
                    to_str(g(r,  "riskometervalue",      "RiskometerValue")),
                    to_str(g(r,  "schemeinvestmenttype", "SchemeInvestmentType")),
                    to_str(g(r,  "schemetype",           "SchemeType")),
                    to_str(g(r,  "groupcode",            "GroupCode")),
                    to_str(g(r,  "groupname",            "GroupName")),
                    to_str(g(r,  "maturitydate",         "MaturityDate")),
                    to_str(g(r,  "lockinperiod",         "LockInPeriod")),
                    to_ts(g(r,   "inceptiondate",        "InceptionDate")),
                    synced_at,
                ))
        except Exception as exc:
            log.warning(f"[{job}] mf_cocode={mf_cocode} fetch failed: {exc}")

    if not all_rows:
        log.warning(f"[{job}] No scheme records collected.")
        return

    execute_values(cur, """
        INSERT INTO scheme_master (
            mf_cocode, amficode, mf_schcode, classcode, category, sch_name,
            navrs, navdate, rtcode, isin, isin_reinvestment, fundmanager,
            launchdate, mininvestment, incrementalinvestment, mininvestment_sip,
            frequency, schemeaum, entrytload, exitload, fundtype, investmenttype,
            mcapcategory, bmcode, benchmarkname, riskometervalue,
            schemeinvestmenttype, schemetype, groupcode, groupname,
            maturitydate, lockinperiod, inceptiondate, synced_at
        ) VALUES %s
        ON CONFLICT (mf_schcode) DO UPDATE SET
            mf_cocode             = EXCLUDED.mf_cocode,
            amficode              = EXCLUDED.amficode,
            classcode             = EXCLUDED.classcode,
            category              = EXCLUDED.category,
            sch_name              = EXCLUDED.sch_name,
            navrs                 = EXCLUDED.navrs,
            navdate               = EXCLUDED.navdate,
            rtcode                = EXCLUDED.rtcode,
            isin                  = EXCLUDED.isin,
            isin_reinvestment     = EXCLUDED.isin_reinvestment,
            fundmanager           = EXCLUDED.fundmanager,
            launchdate            = EXCLUDED.launchdate,
            mininvestment         = EXCLUDED.mininvestment,
            incrementalinvestment = EXCLUDED.incrementalinvestment,
            mininvestment_sip     = EXCLUDED.mininvestment_sip,
            frequency             = EXCLUDED.frequency,
            schemeaum             = EXCLUDED.schemeaum,
            entrytload            = EXCLUDED.entrytload,
            exitload              = EXCLUDED.exitload,
            fundtype              = EXCLUDED.fundtype,
            investmenttype        = EXCLUDED.investmenttype,
            mcapcategory          = EXCLUDED.mcapcategory,
            bmcode                = EXCLUDED.bmcode,
            benchmarkname         = EXCLUDED.benchmarkname,
            riskometervalue       = EXCLUDED.riskometervalue,
            schemeinvestmenttype  = EXCLUDED.schemeinvestmenttype,
            schemetype            = EXCLUDED.schemetype,
            groupcode             = EXCLUDED.groupcode,
            groupname             = EXCLUDED.groupname,
            maturitydate          = EXCLUDED.maturitydate,
            lockinperiod          = EXCLUDED.lockinperiod,
            inceptiondate         = EXCLUDED.inceptiondate,
            synced_at             = EXCLUDED.synced_at
    """, all_rows)

    cur.execute("SELECT COUNT(*) FROM scheme_master;")
    new = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(all_rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── JOB: ETF MASTER ──────────────────────────────────────────────────────────

def sync_etf_master(conn: psycopg2.extensions.connection) -> None:
    job = "etf_master"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM etf_master;")
    before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/ETFMaster")
    log.info(f"[{job}] API returned {len(records)} records.")
    if not records:
        log.warning(f"[{job}] Empty response — skipping.")
        return

    synced_at = datetime.now()
    rows = []
    for r in records:
        equity_cmotscode = to_int(
            r.get("equity_cmotscode") or r.get("EquityCmotsCode") or r.get("Equity_CMOTSCode")
        )
        if not equity_cmotscode:
            continue
        rows.append((
            equity_cmotscode,
            to_int(g(r, "mf_cmotscode", "MF_CMOTSCode",  "MfCmotsCode")),
            to_str(g(r, "amcname",      "AMCName",        "AmcName")),
            to_str(g(r, "etfname",      "ETFName",        "EtfName")),
            to_str(g(r, "isin",         "ISIN",           "Isin")),
            to_str(g(r, "etfcategory",  "ETFCategory",    "EtfCategory")),
            to_int(g(r, "bselisted",    "BSEListed",      "BseListed")),
            to_int(g(r, "nselisted",    "NSEListed",      "NseListed")),
            synced_at,
        ))

    if not rows:
        log.warning(f"[{job}] No valid rows to upsert.")
        return

    execute_values(cur, """
        INSERT INTO etf_master (
            equity_cmotscode, mf_cmotscode, amcname, etfname,
            isin, etfcategory, bselisted, nselisted, synced_at
        ) VALUES %s
        ON CONFLICT (equity_cmotscode) DO UPDATE SET
            mf_cmotscode = EXCLUDED.mf_cmotscode,
            amcname      = EXCLUDED.amcname,
            etfname      = EXCLUDED.etfname,
            isin         = EXCLUDED.isin,
            etfcategory  = EXCLUDED.etfcategory,
            bselisted    = EXCLUDED.bselisted,
            nselisted    = EXCLUDED.nselisted,
            synced_at    = EXCLUDED.synced_at
    """, rows)

    cur.execute("SELECT COUNT(*) FROM etf_master;")
    new = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── JOB: IPO MASTER ──────────────────────────────────────────────────────────

def sync_ipo_master(conn: psycopg2.extensions.connection) -> None:
    job = "ipo_master"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ipo_master;")
    before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/ipomaster")
    log.info(f"[{job}] API returned {len(records)} records.")
    if not records:
        log.warning(f"[{job}] Empty response — skipping.")
        return

    synced_at = datetime.now()
    rows = []
    for r in records:
        co_code = to_int(r.get("co_code") or r.get("CoCode"))
        if not co_code:
            continue
        rows.append((
            co_code,
            to_int(g(r,  "isin",             "ISIN",             "Isin")),
            to_str(g(r,  "companyshortname", "CompanyShortName", "CompanyShortname")),
            to_str(g(r,  "companyname",      "CompanyName")),
            to_str(g(r,  "issue",            "Issue")),
            to_str(g(r,  "issuetype",        "IssueType")),
            to_ts(g(r,   "opendate",         "OpenDate")),
            to_ts(g(r,   "closedate",        "CloseDate")),
            to_str(g(r,  "type",             "Type")),
            to_str(g(r,  "ipotype",          "IPOType",          "IpoType")),
            to_int(g(r,  "freshissue",       "FreshIssue")),
            to_int(g(r,  "mininvestment",    "MinInvestment")),
            synced_at,
        ))

    if not rows:
        log.warning(f"[{job}] No valid rows to upsert.")
        return

    execute_values(cur, """
        INSERT INTO ipo_master (
            co_code, isin, companyshortname, company_name,
            issue, issuetype, opendate, closedate,
            type, ipotype, freshissue, mininvestment, synced_at
        ) VALUES %s
        ON CONFLICT (co_code) DO UPDATE SET
            isin             = EXCLUDED.isin,
            companyshortname = EXCLUDED.companyshortname,
            company_name     = EXCLUDED.company_name,
            issue            = EXCLUDED.issue,
            issuetype        = EXCLUDED.issuetype,
            opendate         = EXCLUDED.opendate,
            closedate        = EXCLUDED.closedate,
            type             = EXCLUDED.type,
            ipotype          = EXCLUDED.ipotype,
            freshissue       = EXCLUDED.freshissue,
            mininvestment    = EXCLUDED.mininvestment,
            synced_at        = EXCLUDED.synced_at
    """, rows)

    cur.execute("SELECT COUNT(*) FROM ipo_master;")
    new = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── JOB: BOND MASTER ─────────────────────────────────────────────────────────
# The API docs list most string fields as Varchar(50) or Varchar(100), but real
# data from the endpoint routinely exceeds those limits — particularly:
#   creditrating  → comma-separated multi-agency strings, easily 200+ chars
#   pcoption      → structured put/call option descriptions, 100–300+ chars
#   tenor         → can include free-text descriptions
#   secdesc       → long security descriptions
# All four are mapped to TEXT in the DDL above to avoid any truncation errors.

def sync_bond_master(conn: psycopg2.extensions.connection) -> None:
    job = "bond_master"
    t0  = time.time()
    log.info(f"[{job}] Starting sync…")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bond_master;")
    before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/BondMaster")
    log.info(f"[{job}] API returned {len(records)} records.")
    if not records:
        log.warning(f"[{job}] Empty response — skipping.")
        return

    synced_at = datetime.now()
    rows = []
    for r in records:
        code = to_int(r.get("code") or r.get("Code"))
        if not code:
            continue
        rows.append((
            code,
            to_str(g(r,   "companyname",   "CompanyName")),
            to_int(g(r,   "bsegroup",      "BSEGroup",     "BseGroup")),
            to_int(g(r,   "bsecode",       "BSECode",      "BseCode")),
            to_str(g(r,   "bsescripname",  "bsescripname", "BseScripName")),
            to_ts(g(r,    "bselistdate",   "bselistdate",  "BseListDate")),
            to_str(g(r,   "nsesymbol",     "NSESymbol",    "NseSymbol")),
            to_str(g(r,   "nseseries",     "NSESeries",    "NseSeries")),
            to_str(g(r,   "nsesecname",    "nsesecname",   "NseSecName")),
            to_ts(g(r,    "nselistdate",   "nselistdate",  "NseListDate")),
            to_str(g(r,   "secdesc",       "SecDesc")),         # TEXT
            to_int(g(r,   "totsecurities", "totSecurities","TotSecurities")),
            to_int(g(r,   "issprice",      "IssPrice")),
            to_int(g(r,   "fv",            "FV")),
            to_int(g(r,   "paidupval",     "PaidupVal")),
            to_int(g(r,   "mktlot",        "Mktlot")),
            to_ts(g(r,    "allotdate",     "AllotDate")),
            to_ts(g(r,    "redeemdate",    "RedemDate",    "RedeemDate")),
            to_int(g(r,   "redeemamt",     "RedemAmt",     "RedeemAmt")),
            to_int(g(r,   "redeemnote",    "RedemNote",    "RedeemNote")),
            to_str(g(r,   "isin",          "ISIN",         "Isin")),
            to_str(g(r,   "tenor",         "Tenor")),            # TEXT
            to_float(g(r, "cprate",        "CPRate")),
            to_str(g(r,   "intfrequency",  "IntFrequency")),
            to_str(g(r,   "pcoption",      "PCOption")),         # TEXT
            to_str(g(r,   "creditrating",  "CreditRating")),     # TEXT
            to_str(g(r,   "bondtype",      "bondtype",     "BondType")),
            synced_at,
        ))

    if not rows:
        log.warning(f"[{job}] No valid rows to upsert.")
        return

    execute_values(cur, """
        INSERT INTO bond_master (
            code, companyname, bsegroup, bsecode, bsescripname, bselistdate,
            nsesymbol, nseseries, nsesecname, nselistdate, secdesc,
            totsecurities, issprice, fv, paidupval, mktlot,
            allotdate, redeemdate, redeemamt, redeemnote,
            isin, tenor, cprate, intfrequency, pcoption,
            creditrating, bondtype, synced_at
        ) VALUES %s
        ON CONFLICT (code) DO UPDATE SET
            companyname    = EXCLUDED.companyname,
            bsegroup       = EXCLUDED.bsegroup,
            bsecode        = EXCLUDED.bsecode,
            bsescripname   = EXCLUDED.bsescripname,
            bselistdate    = EXCLUDED.bselistdate,
            nsesymbol      = EXCLUDED.nsesymbol,
            nseseries      = EXCLUDED.nseseries,
            nsesecname     = EXCLUDED.nsesecname,
            nselistdate    = EXCLUDED.nselistdate,
            secdesc        = EXCLUDED.secdesc,
            totsecurities  = EXCLUDED.totsecurities,
            issprice       = EXCLUDED.issprice,
            fv             = EXCLUDED.fv,
            paidupval      = EXCLUDED.paidupval,
            mktlot         = EXCLUDED.mktlot,
            allotdate      = EXCLUDED.allotdate,
            redeemdate     = EXCLUDED.redeemdate,
            redeemamt      = EXCLUDED.redeemamt,
            redeemnote     = EXCLUDED.redeemnote,
            isin           = EXCLUDED.isin,
            tenor          = EXCLUDED.tenor,
            cprate         = EXCLUDED.cprate,
            intfrequency   = EXCLUDED.intfrequency,
            pcoption       = EXCLUDED.pcoption,
            creditrating   = EXCLUDED.creditrating,
            bondtype       = EXCLUDED.bondtype,
            synced_at      = EXCLUDED.synced_at
    """, rows)

    cur.execute("SELECT COUNT(*) FROM bond_master;")
    new = cur.fetchone()[0] - before
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new} new in {round(time.time()-t0,2)}s")


# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

JOBS = [
    sync_companies,
    sync_group_master,
    sync_fund_house,
    sync_scheme_master,
    sync_etf_master,
    sync_ipo_master,
    sync_bond_master,
]


def run_single(job_fn) -> None:
    """Run one job with a dedicated DB connection."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        job_fn(conn)
    except Exception as exc:
        log.error(f"[{job_fn.__name__}] Failed: {exc}")
    finally:
        if conn:
            conn.close()


def run_all() -> None:
    """Run every job sequentially under one DB connection, preceded by DDL setup."""
    log.info("=" * 60)
    log.info("Starting full sync run")
    t0 = time.time()

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)

        cur = conn.cursor()
        cur.execute(SQL_SETUP)
        conn.commit()
        cur.close()

        for job_fn in JOBS:
            try:
                job_fn(conn)
            except Exception as exc:
                log.error(f"[{job_fn.__name__}] Failed: {exc}")
    finally:
        if conn:
            conn.close()

    log.info(f"Full sync run complete in {round(time.time()-t0, 2)}s")
    log.info("=" * 60)


# ─── SIGNAL HANDLING ──────────────────────────────────────────────────────────

def handle_shutdown(sig, frame) -> None:
    log.info("Shutdown signal received. Exiting.")
    sys.exit(0)


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    log.info("Running initial sync on startup…")
    run_all()

    scheduler = BlockingScheduler(timezone=TIMEZONE)

    # Company master — 4× daily
    scheduler.add_job(
        lambda: run_single(sync_companies),
        CronTrigger(hour="0,6,12,18", minute=0, timezone=TIMEZONE),
        id="company_master",
        name="Company master 4x daily",
        misfire_grace_time=300,
    )

    # Group master — 23:30
    scheduler.add_job(
        lambda: run_single(sync_group_master),
        CronTrigger(hour=23, minute=30, timezone=TIMEZONE),
        id="group_master",
        name="Group master daily",
        misfire_grace_time=300,
    )

    # Fund house — 23:00
    scheduler.add_job(
        lambda: run_single(sync_fund_house),
        CronTrigger(hour=23, minute=0, timezone=TIMEZONE),
        id="fund_house",
        name="Fund house daily",
        misfire_grace_time=300,
    )

    # Scheme master — 23:05 (must run after fund_house)
    scheduler.add_job(
        lambda: run_single(sync_scheme_master),
        CronTrigger(hour=23, minute=5, timezone=TIMEZONE),
        id="scheme_master",
        name="Scheme master daily",
        misfire_grace_time=300,
    )

    # ETF master — 23:35
    scheduler.add_job(
        lambda: run_single(sync_etf_master),
        CronTrigger(hour=23, minute=35, timezone=TIMEZONE),
        id="etf_master",
        name="ETF master daily",
        misfire_grace_time=300,
    )

    # IPO master — 23:30
    scheduler.add_job(
        lambda: run_single(sync_ipo_master),
        CronTrigger(hour=23, minute=30, timezone=TIMEZONE),
        id="ipo_master",
        name="IPO master daily",
        misfire_grace_time=300,
    )

    # Bond master — 23:40
    scheduler.add_job(
        lambda: run_single(sync_bond_master),
        CronTrigger(hour=23, minute=40, timezone=TIMEZONE),
        id="bond_master",
        name="Bond master daily",
        misfire_grace_time=300,
    )

    log.info("Scheduler running. Press Ctrl+C to stop.")
    scheduler.start()