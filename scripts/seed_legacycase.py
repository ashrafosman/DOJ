"""
Seed script: creates and populates the LegacyCase PostgreSQL database.

Tables created:
  public.tbl_defendant  - 500 defendant records
  public.tbl_case       - 300 case master records
  public.tbl_event      - 800 event/hearing records

Run:
  python3 seed_legacycase.py
"""

import os
import random
import subprocess
import json
from datetime import datetime, timedelta

# ── Connection ───────────────────────────────────────────────────────────────

LAKEBASE_HOST = "instance-18144f19-a9b9-447a-bb21-51dc465e9969.database.cloud.databricks.com"
LAKEBASE_DB   = "legacycase"
EMAIL         = "ashraf.osman@databricks.com"

def get_token() -> str:
    result = subprocess.check_output(
        ["databricks", "auth", "token", "--profile", "fe-vm-oregon-doj-demo"],
        text=True,
    )
    return json.loads(result)["access_token"]

# ── Synthetic data generators ─────────────────────────────────────────────────

random.seed(42)

FIRST_NAMES = ["James","Maria","Robert","Linda","Michael","Barbara","William",
               "Patricia","David","Jennifer","Carlos","Ana","Marcus","Tanya",
               "Kevin","Denise","Anthony","Sharon","Eric","Angela","Luis",
               "Sandra","Keith","Yvonne","Brian","Michelle","Gary","Dorothy"]
LAST_NAMES  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
               "Davis","Rodriguez","Martinez","Hernandez","Lopez","Wilson",
               "Anderson","Taylor","Thomas","Moore","Jackson","Martin","Lee",
               "Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez"]
RACE_CODES  = ["W","B","H","A","I","P","OTH","UNK"]
GENDER_CODES= ["M","F"]
CHARGE_CODES= ["PC187","PC211","PC459","PC288","PC245","HS11352","PC664",
               "PC496","PC530.5","PC207","PC422","PC484","PC487","PC594"]
CHARGE_DESC = {
    "PC187":   "Murder/Homicide",     "PC211":   "Robbery",
    "PC459":   "Burglary",            "PC288":   "Lewd Acts on Child",
    "PC245":   "Assault with DW",     "HS11352": "Drug Sale/Transport",
    "PC664":   "Attempted Crime",     "PC496":   "Receiving Stolen Property",
    "PC530.5": "Identity Theft",      "PC207":   "Kidnapping",
    "PC422":   "Criminal Threats",    "PC484":   "Petty Theft",
    "PC487":   "Grand Theft",         "PC594":   "Vandalism",
}
DISP_CODES  = ["CONV","ACQ","DISM","PLEA","NOLO","PEND","DIVER"]
COUNTY_CODES= ["MULT","WASH","CLAC","LANE","JACK","DOUG","BENT","LINC","YAMI","COOS"]
COURTS      = ["C001","C002","C003","C004","C005"]
CASE_TYPES  = ["FELONY","MISDEMEANOR","INFRACTION","JUVENILE"]
STATUSES    = ["OPEN","CLOSED","PENDING","APPEAL","TRANSFERRED"]
EVENT_TYPES = ["ARRAIGNMENT","HEARING","TRIAL","SENTENCING","PLEA",
               "CONTINUANCE","MOTION","VERDICT","BOND HEARING","STATUS CONF"]
EVENT_STATUS= ["COMPLETED","SCHEDULED","CANCELLED","CONTINUED"]

def rand_date(start_year=1970, end_year=2005) -> str:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def rand_case_date() -> str:
    return rand_date(2015, 2024)

def build_case_ids(n: int) -> list[str]:
    ids = set()
    while len(ids) < n:
        ids.add(f"LC-{random.randint(2015,2024)}-{random.randint(10000,99999)}")
    return list(ids)

# Build a shared pool of case IDs so defendants and events reference real cases
CASE_POOL = build_case_ids(300)

def make_defendants(n=500) -> list[tuple]:
    rows = []
    for i in range(1, n + 1):
        charge = random.choice(CHARGE_CODES)
        arraignment = rand_date(2015, 2023)
        has_disp = random.random() > 0.3
        rows.append((
            random.choice(CASE_POOL),           # case_id
            f"DEF-{i:06d}",                     # defendant_id
            random.choice(LAST_NAMES),           # last_name
            random.choice(FIRST_NAMES),          # first_name
            random.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + [""]),  # middle_init
            rand_date(1960, 2000),               # dob
            random.choice(RACE_CODES),           # race_cd
            random.choice(GENDER_CODES),         # gender_cd
            charge,                              # charge_cd
            CHARGE_DESC.get(charge, "Unknown"),  # charge_desc
            random.choice(COURTS),               # court_cd
            f"JDG-{random.randint(100,999)}",    # judge_id
            arraignment,                         # arraignment_dt
            rand_date(2016, 2024) if has_disp else None,  # disposition_dt
            random.choice(DISP_CODES) if has_disp else None,  # disposition_cd
            f"SEN-{random.randint(1,20):02d}" if random.random() > 0.4 else None,  # sentence_cd
            random.choice(COUNTY_CODES),         # county_cd
            random.choice(["OPEN","CLOSED","PENDING","APPEAL"]),  # case_status_cd
            random.randint(0, 5),                # prior_offenses
            random.choice(["Y","N"]),            # public_defender_flg
        ))
    return rows

def make_cases() -> list[tuple]:
    rows = []
    for case_id in CASE_POOL:
        filing = rand_date(2015, 2024)
        rows.append((
            case_id,
            random.choice(CASE_TYPES),
            filing,
            random.choice(STATUSES),
            f"COURT-{random.randint(1,5):03d}",
            f"JUDGE-{random.randint(100,999)}",
            f"DA-{random.randint(1000,9999)}",
        ))
    return rows

def make_events(n=800) -> list[tuple]:
    rows = []
    for i in range(1, n + 1):
        rows.append((
            f"EVT-{i:07d}",
            random.choice(CASE_POOL),
            random.choice(EVENT_TYPES),
            rand_date(2015, 2024),
            f"COURT-{random.randint(1,5):03d}",
            random.choice(EVENT_STATUS),
            f"Note for event {i}" if random.random() > 0.6 else None,
        ))
    return rows

# ── DDL + load ────────────────────────────────────────────────────────────────

DDL = """
DROP TABLE IF EXISTS public.tbl_event;
DROP TABLE IF EXISTS public.tbl_defendant;
DROP TABLE IF EXISTS public.tbl_case;

CREATE TABLE public.tbl_case (
    "CaseID"       VARCHAR(30)  PRIMARY KEY,
    "CASE_TYPE"    VARCHAR(20),
    "FILING_DATE"  VARCHAR(20),
    "STATUS"       VARCHAR(20),
    "COURT_ID"     VARCHAR(20),
    "JUDGE_ID"     VARCHAR(20),
    "DA_ID"        VARCHAR(20)
);

CREATE TABLE public.tbl_defendant (
    "CASE_ID"              VARCHAR(30),
    "DefendantID"          VARCHAR(20) PRIMARY KEY,
    "LAST_NAME"            VARCHAR(50),
    "FIRST_NAME"           VARCHAR(50),
    "MIDDLE_INIT"          VARCHAR(5),
    "DOB"                  VARCHAR(20),
    "RACE_CD"              VARCHAR(10),
    "GENDER_CD"            VARCHAR(5),
    "CHARGE_CD"            VARCHAR(20),
    "CHARGE_DESC"          VARCHAR(100),
    "COURT_CD"             VARCHAR(10),
    "JUDGE_ID"             VARCHAR(20),
    "ARRAIGNMENT_DT"       VARCHAR(20),
    "DISPOSITION_DT"       VARCHAR(20),
    "DISPOSITION_CD"       VARCHAR(10),
    "SENTENCE_CD"          VARCHAR(10),
    "COUNTY_CD"            VARCHAR(10),
    "CASE_STATUS_CD"       VARCHAR(15),
    "PRIOR_OFFENSES"       INTEGER,
    "PUBLIC_DEFENDER_FLG"  VARCHAR(3),
    FOREIGN KEY ("CASE_ID") REFERENCES public.tbl_case ("CaseID")
);

CREATE TABLE public.tbl_event (
    "EventID"       VARCHAR(20) PRIMARY KEY,
    "CASE_ID"       VARCHAR(30),
    "EVENT_TYPE"    VARCHAR(30),
    "EVENT_DATE"    VARCHAR(20),
    "COURT_ID"      VARCHAR(20),
    "EVENT_STATUS"  VARCHAR(20),
    "NOTES"         TEXT,
    FOREIGN KEY ("CASE_ID") REFERENCES public.tbl_case ("CaseID")
);

CREATE INDEX idx_defendant_case ON public.tbl_defendant ("CASE_ID");
CREATE INDEX idx_event_case     ON public.tbl_event     ("CASE_ID");
CREATE INDEX idx_case_status    ON public.tbl_case      ("STATUS");
CREATE INDEX idx_def_status     ON public.tbl_defendant ("CASE_STATUS_CD");
"""


def run():
    import psycopg2
    import psycopg2.extras

    token = get_token()
    conn_str = (
        f"host={LAKEBASE_HOST} port=5432 dbname={LAKEBASE_DB} "
        f"user={EMAIL} password={token} sslmode=require"
    )
    print(f"Connecting to {LAKEBASE_HOST}/{LAKEBASE_DB} …")
    conn = psycopg2.connect(conn_str)
    conn.autocommit = False
    cur  = conn.cursor()

    print("Creating schema …")
    cur.execute(DDL)
    conn.commit()

    # Cases (must be inserted first — FK target)
    cases = make_cases()
    print(f"Inserting {len(cases)} cases …")
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO public.tbl_case
           ("CaseID","CASE_TYPE","FILING_DATE","STATUS","COURT_ID","JUDGE_ID","DA_ID")
           VALUES %s""",
        cases, page_size=200,
    )
    conn.commit()

    # Defendants
    defendants = make_defendants(500)
    print(f"Inserting {len(defendants)} defendants …")
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO public.tbl_defendant
           ("CASE_ID","DefendantID","LAST_NAME","FIRST_NAME","MIDDLE_INIT","DOB",
            "RACE_CD","GENDER_CD","CHARGE_CD","CHARGE_DESC","COURT_CD","JUDGE_ID",
            "ARRAIGNMENT_DT","DISPOSITION_DT","DISPOSITION_CD","SENTENCE_CD",
            "COUNTY_CD","CASE_STATUS_CD","PRIOR_OFFENSES","PUBLIC_DEFENDER_FLG")
           VALUES %s""",
        defendants, page_size=200,
    )
    conn.commit()

    # Events
    events = make_events(800)
    print(f"Inserting {len(events)} events …")
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO public.tbl_event
           ("EventID","CASE_ID","EVENT_TYPE","EVENT_DATE","COURT_ID","EVENT_STATUS","NOTES")
           VALUES %s""",
        events, page_size=200,
    )
    conn.commit()

    # Summary
    for tbl in ["tbl_case", "tbl_defendant", "tbl_event"]:
        cur.execute(f'SELECT COUNT(*) FROM public."{tbl}"')
        cnt = cur.fetchone()[0]
        print(f"  {tbl}: {cnt:,} rows")

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    run()
