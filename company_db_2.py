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
import os
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_URL  = "https://equifizapis.cmots.com/api"
JWT_TOKEN = os.getenv("JWT_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImVxdWlmaXphcGlzIiwicm9sZSI6IkFkbWluIiwibmJmIjoxNzc2OTMyODcyLCJleHAiOjE4MDkxNjAwNzIsImlhdCI6MTc3NjkzMjg3MiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDo1MDE5MSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NTAxOTEifQ.lz6do_yCsDQTFz5E-qi4w825YvjFY7lWv_l1qWG4W9I")
TIMEZONE  = "Asia/Kolkata"
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

CREATE TABLE IF NOT EXISTS group_master (
    indexcode  INTEGER PRIMARY KEY,
    exchange   VARCHAR(50),
    group_name VARCHAR(50),
    synced_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fund_house (
    mf_cocode   INTEGER PRIMARY KEY,
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
    mf_schcode            INTEGER PRIMARY KEY,
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
    equity_cmotscode INTEGER PRIMARY KEY,   -- Equity CMOTS Code (unique ETF identifier)
    mf_cmotscode     INTEGER,               -- MF CMOTS Code (links to fund_house.mf_cocode)
    amcname          VARCHAR(50),           -- AMC Name
    etfname          VARCHAR(200),          -- ETF Name
    isin             VARCHAR(50),           -- ISIN
    etfcategory      VARCHAR(50),           -- ETF Category (e.g. Equity, Debt, Gold, etc.)
    bselisted        INTEGER,               -- BSE Listed flag (1 = yes, 0 = no)
    nselisted        INTEGER,               -- NSE Listed flag (1 = yes, 0 = no)
    synced_at        TIMESTAMP DEFAULT NOW()
);
"""

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_session():
    session = requests.Session()
    retry   = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch(session, url):
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    resp    = session.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    for key in ("data", "Data", "result", "Result"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def g(record, *keys):
    """Return first non-None value from record by trying multiple key casings."""
    for k in keys:
        v = record.get(k)
        if v is not None:
            return v
    return ""


def to_int(val):
    try:
        return int(val) if val not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None


def to_float(val):
    try:
        return float(val) if val not in (None, "", "N/A") else None
    except (ValueError, TypeError):
        return None


def to_str(val):
    if val in (None, ""):
        return None
    return str(val)


# ─── JOB: COMPANY MASTER ──────────────────────────────────────────────────────

def sync_companies(conn):
    job       = "company_master"
    run_start = time.time()
    log.info(f"[{job}] Starting sync...")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM companies;")
    count_before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/CompanyMaster")
    log.info(f"[{job}] API returned {len(records)} records.")

    if not records:
        log.warning(f"[{job}] API returned empty list")
        return

    synced_at = datetime.now()
    rows      = []
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
    new_rows = cur.fetchone()[0] - count_before
    duration = round(time.time() - run_start, 2)
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# ─── JOB: GROUP MASTER ────────────────────────────────────────────────────────

def sync_group_master(conn):
    job       = "group_master"
    run_start = time.time()
    log.info(f"[{job}] Starting sync...")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM group_master;")
    count_before = cur.fetchone()[0]

    session = get_session()
    records = []
    for exchange in ("BSE", "NSE"):
        data = fetch(session, f"{BASE_URL}/GroupMaster/{exchange}")
        log.info(f"[{job}] {exchange} returned {len(data)} records.")
        records.extend(data)

    if not records:
        log.warning(f"[{job}] API returned empty list")
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
    new_rows = cur.fetchone()[0] - count_before
    duration = round(time.time() - run_start, 2)
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# ─── JOB: FUND HOUSE ──────────────────────────────────────────────────────────

def sync_fund_house(conn):
    job       = "fund_house"
    run_start = time.time()
    log.info(f"[{job}] Starting sync...")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fund_house;")
    count_before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/Fund_House")
    log.info(f"[{job}] API returned {len(records)} records.")

    if not records:
        log.warning(f"[{job}] API returned empty list")
        return

    synced_at = datetime.now()
    rows      = []
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
            g(r, "telephone",   "Telephone")   or None,
            g(r, "website",     "Website"),
            g(r, "email",       "Email"),
            g(r, "osch",        "Osch")        or None,
            g(r, "csch",        "Csch")        or None,
            g(r, "isch",        "Isch")        or None,
            g(r, "started_on",  "StartedOn")   or None,
            g(r, "sumoftotnav", "SumOfTotNav") or None,
            g(r, "dateas",      "DateAs")      or None,
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
    new_rows = cur.fetchone()[0] - count_before
    duration = round(time.time() - run_start, 2)
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# ─── JOB: SCHEME MASTER ───────────────────────────────────────────────────────

def sync_scheme_master(conn):
    job       = "scheme_master"
    run_start = time.time()
    log.info(f"[{job}] Starting sync...")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM scheme_master;")
    count_before = cur.fetchone()[0]

    cur.execute("SELECT mf_cocode FROM fund_house;")
    mf_cocodes = [row[0] for row in cur.fetchall()]

    if not mf_cocodes:
        log.warning(f"[{job}] No fund houses found — skipping scheme master sync")
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
                    to_int(r.get("mf_cocode")  or r.get("MfCoCode")  or mf_cocode),
                    to_int(g(r, "amficode",     "AmfiCode")),
                    mf_schcode,
                    to_int(g(r, "classcode",    "ClassCode")),
                    to_str(g(r, "category",     "Category")),
                    to_str(g(r, "sch_name",     "SchName")),
                    to_float(g(r, "navrs",      "Navrs")),
                    to_str(g(r, "navdate",      "NavDate"))   or None,
                    to_int(g(r, "rtcode",       "RtCode")),
                    to_str(g(r, "isin",         "Isin")),
                    to_str(g(r, "isin_reinvestment", "IsinReinvestment")),
                    to_str(g(r, "fundmanager",  "FundManager")),
                    to_str(g(r, "launchdate",   "LaunchDate")) or None,
                    to_float(g(r, "mininvestment",         "MinInvestment")),
                    to_float(g(r, "incrementalinvestment", "IncrementalInvestment")),
                    to_float(g(r, "mininvestment_sip",     "MinInvestmentSip")),
                    to_str(g(r, "frequency",    "Frequency")),
                    to_float(g(r, "schemeaum",  "SchemeAum")),
                    to_str(g(r, "entrytload",   "EntrytLoad")),
                    to_str(g(r, "exitload",     "ExitLoad")),
                    to_str(g(r, "fundtype",     "FundType")),
                    to_str(g(r, "investmenttype", "InvestmentType")),
                    to_str(g(r, "mcapcategory", "McapCategory")),
                    to_int(g(r, "bmcode",       "BmCode")),
                    to_str(g(r, "benchmarkname","BenchmarkName")),
                    to_str(g(r, "riskometervalue", "RiskometerValue")),
                    to_str(g(r, "schemeinvestmenttype", "SchemeInvestmentType")),
                    to_str(g(r, "schemetype",   "SchemeType")),
                    to_str(g(r, "groupcode",    "GroupCode")),
                    to_str(g(r, "groupname",    "GroupName")),
                    to_str(g(r, "maturitydate", "MaturityDate")),
                    to_str(g(r, "lockinperiod", "LockInPeriod")),
                    to_str(g(r, "inceptiondate","InceptionDate")) or None,
                    synced_at,
                ))
        except Exception as e:
            log.warning(f"[{job}] mf_cocode={mf_cocode} fetch failed: {e}")
            continue

    if not all_rows:
        log.warning(f"[{job}] No scheme records collected")
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
    new_rows = cur.fetchone()[0] - count_before
    duration = round(time.time() - run_start, 2)
    conn.commit()
    log.info(f"[{job}] Done — {len(all_rows)} upserted, {new_rows} new in {duration}s")


# ─── JOB: ETF MASTER ──────────────────────────────────────────────────────────
# Source : GET /api/ETFMaster
# Schedule: EOD, once on trading day (11:30 PM – 11:55 PM IST)
# Primary key: equity_cmotscode

def sync_etf_master(conn):
    job       = "etf_master"
    run_start = time.time()
    log.info(f"[{job}] Starting sync...")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM etf_master;")
    count_before = cur.fetchone()[0]

    records = fetch(get_session(), f"{BASE_URL}/ETFMaster")
    log.info(f"[{job}] API returned {len(records)} records.")

    if not records:
        log.warning(f"[{job}] API returned empty list")
        return

    synced_at = datetime.now()
    rows      = []
    for r in records:
        equity_cmotscode = to_int(
            r.get("equity_cmotscode") or r.get("EquityCmotsCode") or r.get("Equity_CMOTSCode")
        )
        if not equity_cmotscode:
            log.debug(f"[{job}] Skipping record with no equity_cmotscode: {r}")
            continue
        rows.append((
            equity_cmotscode,
            to_int(g(r,  "mf_cmotscode",  "MF_CMOTSCode",   "MfCmotsCode")),
            to_str(g(r,  "amcname",       "AMCName",         "AmcName")),
            to_str(g(r,  "etfname",       "ETFName",         "EtfName")),
            to_str(g(r,  "isin",          "ISIN",            "Isin")),
            to_str(g(r,  "etfcategory",   "ETFCategory",     "EtfCategory")),
            to_int(g(r,  "bselisted",     "BSEListed",       "BseListed")),
            to_int(g(r,  "nselisted",     "NSEListed",       "NseListed")),
            synced_at,
        ))

    if not rows:
        log.warning(f"[{job}] No valid rows to upsert")
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
    new_rows = cur.fetchone()[0] - count_before
    duration = round(time.time() - run_start, 2)
    conn.commit()
    log.info(f"[{job}] Done — {len(rows)} upserted, {new_rows} new in {duration}s")


# ─── ORCHESTRATOR ─────────────────────────────────────────────────────────────

JOBS = [
    sync_companies,
    sync_group_master,
    sync_fund_house,
    sync_scheme_master,
    sync_etf_master,        # ← new
]


def run_single(job_fn):
    """Run a single job with its own DB connection."""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        job_fn(conn)
    except Exception as e:
        log.error(f"[{job_fn.__name__}] Failed: {e}")
    finally:
        if conn:
            conn.close()


def run_all():
    log.info("=" * 60)
    log.info("Starting full sync run")
    total_start = time.time()

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
            except Exception as e:
                log.error(f"[{job_fn.__name__}] Failed: {e}")
    finally:
        if conn:
            conn.close()

    log.info(f"Full sync run complete in {round(time.time() - total_start, 2)}s")
    log.info("=" * 60)


# ─── SCHEDULER ────────────────────────────────────────────────────────────────

def handle_shutdown(sig, frame):
    log.info("Shutdown signal received. Exiting.")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT,  handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    log.info("Running initial sync on startup...")
    run_all()

    scheduler = BlockingScheduler(timezone=TIMEZONE)

    # Company master: 4x daily
    scheduler.add_job(
        lambda: run_single(sync_companies),
        CronTrigger(hour="0,6,12,18", minute=0, timezone=TIMEZONE),
        id="company_master",
        name="Company master 4x daily",
        misfire_grace_time=300,
    )

    # Group master: once daily at 11:30 PM
    scheduler.add_job(
        lambda: run_single(sync_group_master),
        CronTrigger(hour=23, minute=30, timezone=TIMEZONE),
        id="group_master",
        name="Group master daily",
        misfire_grace_time=300,
    )

    # Fund house: once daily at 11:00 PM
    scheduler.add_job(
        lambda: run_single(sync_fund_house),
        CronTrigger(hour=23, minute=0, timezone=TIMEZONE),
        id="fund_house",
        name="Fund house daily",
        misfire_grace_time=300,
    )

    # Scheme master: once daily at 11:05 PM (after fund_house)
    scheduler.add_job(
        lambda: run_single(sync_scheme_master),
        CronTrigger(hour=23, minute=5, timezone=TIMEZONE),
        id="scheme_master",
        name="Scheme master daily",
        misfire_grace_time=300,
    )

    # ETF master: once daily at 11:35 PM (EOD, 11:30–11:55 PM window per API docs)
    scheduler.add_job(
        lambda: run_single(sync_etf_master),
        CronTrigger(hour=23, minute=35, timezone=TIMEZONE),
        id="etf_master",
        name="ETF master daily",
        misfire_grace_time=300,
    )

    log.info("Scheduler running. Press Ctrl+C to stop.")
    scheduler.start()