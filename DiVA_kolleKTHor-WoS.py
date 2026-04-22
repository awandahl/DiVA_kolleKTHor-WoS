#!/usr/bin/env python3
import time
import re
import requests
import pandas as pd
from tqdm import tqdm  # pip install tqdm
from urllib.parse import quote
from datetime import datetime

# -------------------- CONFIG --------------------

FROM_YEAR = 2001
TO_YEAR = 2002

# which DiVA portal to use: e.g. "kth", "uu", "umu", "lnu", etc.
DIVA_PORTAL = "kth"
DIVA_BASE = f"https://{DIVA_PORTAL}.diva-portal.org/smash/export.jsf"

# We only care about records with missing ISI IDs
MISSING_ISI_ONLY = True  # rows with empty ISI

# WoS Starter API
WOS_BASE = "https://api.clarivate.com/apis/wos-starter/v1/documents"
WOS_API_KEY = ""  # put your API key here or inject from env/CLI later
WOS_DB = "WOS"
WOS_LIMIT = 5
SLEEP_SECONDS = 1.0

# Matching
SIM_THRESHOLD = 0.9
MAX_ACCEPTED = 9999

# Filenames: portal + year range (+ timestamp for outputs)
TIMESTAMP = datetime.now().strftime("%Y%m%d-%H%M%S")
PREFIX = f"{DIVA_PORTAL}_{FROM_YEAR}-{TO_YEAR}"

DOWNLOADED_CSV = f"{PREFIX}_diva_raw.csv"                      # input snapshot
OUTPUT_CSV = f"{PREFIX}_diva_wos_uid_candidates_{TIMESTAMP}.csv"   # output with timestamp
EXCEL_OUT = f"{PREFIX}_diva_wos_uid_candidates_{TIMESTAMP}.xlsx"   # output with timestamp

ISBN_RE = re.compile(r"\b(?:97[89][- ]?)?\d[-\d ]{8,}\d\b")


# -------------------- HELPERS --------------------


def build_diva_url(from_year: int, to_year: int) -> str:
    aq = f'[[{{"dateIssued":{{"from":"{from_year}","to":"{to_year}"}}}}]]'
    aq2 = (
        '[[{"publicationTypeCode":["bookReview","review","article","book",'
        '"chapter","conferencePaper"]}]]'
    )

    params = {
        "format": "csv",
        "addFilename": "true",
        "aq": aq,
        "aqe": "[]",
        "aq2": aq2,
        "onlyFullText": "false",
        "noOfRows": "99999",
        "sortOrder": "title_sort_asc",
        "sortOrder2": "title_sort_asc",
        "csvType": "publication",
        "fl": (
            "PID,ArticleId,DOI,EndPage,ISBN,ISBN_ELECTRONIC,ISBN_PRINT,ISBN_UNDEFINED,"
            "ISI,Issue,Journal,JournalEISSN,JournalISSN,Pages,PublicationType,PMID,"
            "ScopusId,SeriesEISSN,SeriesISSN,StartPage,Title,Name,Volume,Year,Notes"
        ),
    }

    encoded = [f"{k}={quote(v, safe='')}" for k, v in params.items()]
    return DIVA_BASE + "?" + "&".join(encoded)


def download_diva_csv(url: str, out_path: str):
    print(f"Downloading DiVA CSV from {url}")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        )
    }
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)
    print(f"Saved DiVA CSV to {out_path}")


def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = "".join(ch for ch in s if ch.isprintable())
    return s.strip()


def normalize_title(t: str) -> list[str]:
    t = clean_text(t).lower()
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return [tok for tok in t.split() if tok]


def title_similarity(a: str, b: str) -> float:
    ta = set(normalize_title(a))
    tb = set(normalize_title(b))
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union


def normalize_page(page_str: str) -> str:
    if not page_str:
        return ""
    page_str = str(page_str).strip()
    if page_str.isdigit():
        return str(int(page_str))
    return page_str


def norm_issn(s: str) -> str:
    s = (s or "").strip()
    return s.replace("-", "")


def norm_isbn(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^0-9Xx]", "", s)
    return s.upper()


# ---- Publication type mapping ----


def diva_pubtype_category(diva_type: str) -> str | None:
    t = (diva_type or "").strip().lower()

    if t in {
        "article",
        "article in journal",
        "review",
        "bookreview",
        "book review",
    }:
        return "article"

    if t in {
        "conferencepaper",
        "conference paper",
        "paper in conference proceeding",
        "paper in conference proceedings",
    }:
        return "conference"

    if t in {
        "chapter",
        "chapter in book",
        "chapter in anthology",
    }:
        return "chapter"

    if t in {
        "book",
        "monograph",
    }:
        return "book"

    if t == "article":
        return "article"
    if t == "conferencepaper":
        return "conference"
    if t == "book":
        return "book"
    if t == "chapter":
        return "chapter"

    return None


def wos_document_type_category(doc_type_value) -> str | None:
    values: list[str] = []
    if isinstance(doc_type_value, list):
        values = [str(x).strip().lower() for x in doc_type_value if str(x).strip()]
    elif doc_type_value:
        values = [str(doc_type_value).strip().lower()]

    for t in values:
        if t in {"article", "review", "book review", "journal article"}:
            return "article"
        if t in {"proceedings paper", "conference paper"}:
            return "conference"
        if t in {"book", "monograph"}:
            return "book"
        if t in {"book chapter", "chapter"}:
            return "chapter"
    return None


# ---- Author helpers ----


def extract_diva_author_names(raw: str) -> list[str]:
    if not raw:
        return []
    authors: list[str] = []
    for part in raw.split(";"):
        part = part.strip()
        if not part:
            continue
        part = re.split(r"\s\(", part, maxsplit=1)[0]
        part = re.sub(r"\[[^\]]*\]", "", part).strip()
        part = re.sub(r"\s+", " ", part)
        if part:
            authors.append(part)
    return authors


def extract_diva_authors(row) -> set[str]:
    raw = (row.get("Name", "") or "").strip()
    names = extract_diva_author_names(raw)
    surnames: set[str] = set()
    for n in names:
        fam = n.split(",", 1)[0].strip().lower()
        if fam:
            surnames.add(fam)
    return surnames


def extract_wos_authors(metadata: dict) -> set[str]:
    authors = metadata.get("names") or metadata.get("authors") or []
    names: set[str] = set()
    for a in authors:
        if isinstance(a, dict):
            full = (a.get("displayName") or a.get("fullName") or a.get("name") or "").strip()
            if not full:
                full = " ".join(
                    x for x in [a.get("lastName", ""), a.get("firstName", "")] if x
                ).strip()
        else:
            full = str(a).strip()
        if not full:
            continue
        full = re.sub(r"\s+", " ", full)
        if "," in full:
            fam = full.split(",", 1)[0].strip().lower()
        else:
            fam = full.split()[-1].strip().lower() if full.split() else ""
        if fam:
            names.add(fam)
    return names


def authors_match(diva_row, metadata: dict) -> bool:
    diva_auth = extract_diva_authors(diva_row)
    wos_auth = extract_wos_authors(metadata)

    if not diva_auth or not wos_auth:
        print(" ⚠ Missing authors on one side; skipping author check")
        return False

    inter = diva_auth & wos_auth
    print(f" DiVA authors: {sorted(diva_auth)}")
    print(f" WoS authors: {sorted(wos_auth)}")
    print(f" Author intersection: {sorted(inter)}")
    return bool(inter)


# ---- Host ISBN helpers for conference/chapter ----


def extract_host_isbns(row) -> set[str]:
    candidates: list[str] = []

    for col in ["ISBN", "ISBN_PRINT", "ISBN_ELECTRONIC"]:
        v = (row.get(col, "") or "").strip()
        if v:
            candidates.append(v)

    notes = (row.get("Notes", "") or "")
    for match in ISBN_RE.findall(notes):
        candidates.append(match)

    norm = {norm_isbn(c) for c in candidates if c}
    return {x for x in norm if len(x) >= 10}


def extract_diva_book_isbns(row) -> set[str]:
    candidates: list[str] = []
    for col in ["ISBN", "ISBN_PRINT", "ISBN_ELECTRONIC"]:
        v = (row.get(col, "") or "").strip()
        if v:
            candidates.append(v)
    norm = {norm_isbn(c) for c in candidates if c}
    return {x for x in norm if len(x) >= 10}


def extract_wos_isbns(metadata: dict) -> set[str]:
    candidates: list[str] = []
    ids = metadata.get("identifiers") or {}
    for key in ["isbn", "eisbn"]:
        v = ids.get(key)
        if isinstance(v, list):
            candidates.extend(str(x) for x in v if x)
        elif v:
            candidates.append(str(v))

    source = metadata.get("source") or {}
    for key in ["isbn", "eisbn"]:
        v = source.get(key)
        if isinstance(v, list):
            candidates.extend(str(x) for x in v if x)
        elif v:
            candidates.append(str(v))

    norm = {norm_isbn(s or "") for s in candidates}
    return {x for x in norm if len(x) >= 10}


# ---- WoS helpers ----


def wos_headers() -> dict[str, str]:
    return {
        "accept": "application/json",
        "X-ApiKey": WOS_API_KEY,
    }


def search_wos(query: str, limit: int = 5, page: int = 1) -> list[dict]:
    params = {
        "db": WOS_DB,
        "q": query,
        "limit": str(limit),
        "page": str(page),
    }
    r = requests.get(WOS_BASE, headers=wos_headers(), params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("hits", []) or []


def search_wos_doi(doi: str, limit: int = 5):
    q = f"DO={doi.strip()}"
    return q, search_wos(q, limit=limit, page=1)


def search_wos_title(title: str, year: int | None = None, max_results: int = 5):
    q = f'TI="{clean_text(title)}"'
    if year:
        q += f" AND PY={year}"
    return q, search_wos(q, limit=max_results, page=1)


def get_wos_full_metadata(uid: str) -> dict:
    url = f"{WOS_BASE}/{quote(uid, safe='')}"
    try:
        r = requests.get(url, headers=wos_headers(), timeout=20)
        r.raise_for_status()
        return r.json() or {}
    except Exception as e:
        print(f" ERROR fetching full metadata for {uid}: {e}")
        return {}


def extract_wos_title(hit: dict) -> str:
    return clean_text(hit.get("title", "") or hit.get("sourceTitle", "") or "")


def extract_wos_year(hit: dict) -> int | None:
    for key in ["publishYear", "year"]:
        val = hit.get(key)
        try:
            if val is not None and str(val).strip() != "":
                return int(str(val).strip())
        except Exception:
            pass
    source = hit.get("source") or {}
    for key in ["publishYear", "year"]:
        val = source.get(key)
        try:
            if val is not None and str(val).strip() != "":
                return int(str(val).strip())
        except Exception:
            pass
    return None


def extract_wos_uid(hit: dict) -> str:
    return (hit.get("uid", "") or "").strip()


def extract_wos_doi(hit: dict) -> str:
    ids = hit.get("identifiers") or {}
    return (ids.get("doi", "") or hit.get("doi", "") or "").strip()


def extract_wos_biblio(metadata: dict) -> dict:
    source = metadata.get("source") or {}
    ids = metadata.get("identifiers") or {}

    volume = source.get("volume", "") or metadata.get("volume", "") or ""
    issue = source.get("issue", "") or metadata.get("issue", "") or ""

    start_page = ""
    end_page = ""
    page = source.get("pages") or metadata.get("pages") or ""

    if isinstance(page, dict):
        start_page = page.get("begin", "") or page.get("start", "") or ""
        end_page = page.get("end", "") or ""
    elif isinstance(page, str) and page:
        if "-" in page:
            parts = page.split("-", 1)
            start_page = parts[0].strip()
            if len(parts) > 1:
                end_page = parts[1].strip()
        else:
            start_page = page.strip()

    if not start_page:
        start_page = metadata.get("pageStart", "") or source.get("pageStart", "") or ""
    if not end_page:
        end_page = metadata.get("pageEnd", "") or source.get("pageEnd", "") or ""

    issn_candidates: list[str] = []
    for obj in [ids, source, metadata]:
        for key in ["issn", "eissn", "ISSN", "EISSN"]:
            v = obj.get(key)
            if isinstance(v, list):
                issn_candidates.extend(str(x) for x in v if x)
            elif v:
                issn_candidates.append(str(v))

    issn_set = {norm_issn(x) for x in issn_candidates if norm_issn(x)}
    container_title = clean_text(source.get("sourceTitle", "") or metadata.get("sourceTitle", "") or "")

    return {
        "volume": normalize_page(volume),
        "issue": normalize_page(issue),
        "start_page": normalize_page(start_page),
        "end_page": normalize_page(end_page),
        "issns": issn_set,
        "container_title": container_title,
    }


def issn_match(diva_row, wos_biblio: dict) -> bool:
    diva_issns = {
        norm_issn(diva_row.get(col, ""))
        for col in ["JournalISSN", "JournalEISSN", "SeriesISSN", "SeriesEISSN"]
        if norm_issn(diva_row.get(col, ""))
    }

    wos_issns = wos_biblio.get("issns", set()) or set()

    if not diva_issns or not wos_issns:
        print(" ⚠ Missing ISSN on one side; cannot ISSN-match")
        return False

    inter = diva_issns & wos_issns
    print(f" DiVA ISSNs: {sorted(diva_issns)}")
    print(f" WoS ISSNs: {sorted(wos_issns)}")
    print(f" ISSN intersection: {sorted(inter)}")
    return bool(inter)


def bibliographic_match(diva_row, wos_biblio: dict) -> bool:
    diva_volume = normalize_page(diva_row.get("Volume", ""))
    diva_issue = normalize_page(diva_row.get("Issue", ""))
    diva_start = normalize_page(diva_row.get("StartPage", ""))
    diva_end = normalize_page(diva_row.get("EndPage", ""))

    wos_volume = wos_biblio.get("volume", "")
    wos_issue = wos_biblio.get("issue", "")
    wos_start = wos_biblio.get("start_page", "")
    wos_end = wos_biblio.get("end_page", "")

    checks = []

    if diva_volume and wos_volume:
        checks.append(("Volume", diva_volume == wos_volume, diva_volume, wos_volume))
    if diva_issue and wos_issue:
        checks.append(("Issue", diva_issue == wos_issue, diva_issue, wos_issue))
    if diva_start and wos_start:
        checks.append(("StartPage", diva_start == wos_start, diva_start, wos_start))
    if diva_end and wos_end:
        checks.append(("EndPage", diva_end == wos_end, diva_end, wos_end))

    for field, matches, diva_val, wos_val in checks:
        status = "✓" if matches else "✗"
        print(f" {status} {field}: DiVA='{diva_val}' vs WoS='{wos_val}'")

    if not checks:
        print(" ⚠ No bibliographic fields to compare")
        return False

    return all(check[1] for check in checks)


def make_doi_url(doi: str) -> str:
    doi = (doi or "").strip()
    if not doi:
        return ""
    return f"https://doi.org/{doi}"


def make_pid_url(pid: str) -> str:
    pid = (pid or "").strip()
    if not pid:
        return ""
    if pid.isdigit():
        pid_value = f"diva2:{pid}"
    else:
        pid_value = pid
    encoded_pid = quote(pid_value, safe="")
    return f"https://{DIVA_PORTAL}.diva-portal.org/smash/record.jsf?pid={encoded_pid}"


def make_wos_uid_url(uid: str) -> str:
    uid = (uid or "").strip()
    if not uid:
        return ""
    encoded_uid = quote(uid, safe="")
    return f"https://www.webofscience.com/wos/woscc/full-record/{encoded_uid}"


# -------------------- MAIN --------------------


def main():
    if not WOS_API_KEY.strip():
        raise ValueError("Please set WOS_API_KEY before running the script")

    url = build_diva_url(FROM_YEAR, TO_YEAR)
    download_diva_csv(url, DOWNLOADED_CSV)

    df = pd.read_csv(DOWNLOADED_CSV, dtype=str).fillna("")
    df["ISI"] = df["ISI"].astype(str).str.strip()
    df["DOI"] = df["DOI"].astype(str).str.strip()
    df["Title"] = df["Title"].apply(clean_text)

    for col in [
        "Possible_WOS_UID",
        "Verified_WOS_UID",
        "Possible_WOS_DOI",
        "Verified_WOS_DOI",
        "Check_ISSN_OK",
        "Check_Biblio_OK",
        "Check_Authors_OK",
        "Check_HostISBN_OK",
        "Check_BookISBN_OK",
        "Check_Category",
        "Check_Title_OK",
        "Check_Year_OK",
        "WOS_Query",
        "WOS_Match_Method",
    ]:
        if col not in df.columns:
            df[col] = ""

    def to_int_or_none(s: str):
        try:
            return int(str(s).strip())
        except Exception:
            return None

    year_int = df["Year"].apply(to_int_or_none)
    year_mask = year_int.between(FROM_YEAR, TO_YEAR, inclusive="both")
    df = df[year_mask].copy()
    print(f"After Year filter {FROM_YEAR}-{TO_YEAR}: {len(df)} rows")

    exclude_titles = {"foreword", "preface"}
    df = df[~df["Title"].str.strip().str.lower().isin(exclude_titles)].copy()
    print(f"After excluding Foreword/Preface: {len(df)} rows")

    missing_isi_mask = df["ISI"].str.strip() == ""
    if MISSING_ISI_ONLY:
        working_mask = missing_isi_mask
    else:
        raise ValueError("This script is intended to run with MISSING_ISI_ONLY=True only")

    working_mask &= (df["Title"].str.strip() != "") & (df["Year"].str.strip() != "")
    df_work = df[working_mask].copy()
    print(f"Working rows (missing ISI): {len(df_work)}")

    accepted_count = 0

    # ---- ROUND 1: DOI -> WoS UID ----
    round1_mask = df_work["DOI"].str.strip() != ""
    round1_index = list(df_work[round1_mask].index)
    print(f"Round 1 rows (DOI present, missing ISI): {len(round1_index)}")

    for idx in tqdm(round1_index, desc="Round 1 DOI -> WoS"):
        if accepted_count >= MAX_ACCEPTED:
            print(f"\nReached MAX_ACCEPTED={MAX_ACCEPTED}, stopping early.")
            break

        try:
            row = df_work.loc[idx]
            pid = row["PID"].strip()
            doi = row["DOI"].strip()
            title = row["Title"].strip()
            year_str = row["Year"].strip()

            try:
                pub_year = int(year_str)
            except Exception:
                pub_year = None

            print(f"\n[ROUND1 {idx}] PID={pid}")
            print(f" Title: '{title}'")
            print(f" DOI: {doi}")
            print(f" Year: {pub_year}")
            print(" -> querying WoS by DOI...")

            try:
                query_used, hits = search_wos_doi(doi, limit=WOS_LIMIT)
            except Exception as e:
                print(f" ERROR querying WoS: {e}")
                time.sleep(SLEEP_SECONDS)
                continue

            df_work.at[idx, "WOS_Query"] = query_used
            df_work.at[idx, "WOS_Match_Method"] = "doi_round"

            if not hits:
                print(" No DOI hits found in WoS")
                time.sleep(SLEEP_SECONDS)
                continue

            best_uid = None
            best_doi = None
            best_title_ok = False
            best_year_ok = False

            for hit in hits:
                uid = extract_wos_uid(hit)
                hit_doi = extract_wos_doi(hit)
                hit_title = extract_wos_title(hit)
                hit_year = extract_wos_year(hit)
                sim = title_similarity(title, hit_title)
                year_ok = (pub_year is not None and hit_year == pub_year)
                title_ok = (sim >= SIM_THRESHOLD) if hit_title else False

                print(f" cand uid={uid} doi={hit_doi} year={hit_year} sim={sim:.3f}")

                if hit_doi and hit_doi.lower() == doi.lower():
                    best_uid = uid
                    best_doi = hit_doi
                    best_title_ok = title_ok
                    best_year_ok = year_ok
                    break

            if best_uid:
                df_work.at[idx, "Verified_WOS_UID"] = best_uid
                df_work.at[idx, "Verified_WOS_DOI"] = best_doi
                df_work.at[idx, "Possible_WOS_UID"] = ""
                df_work.at[idx, "Possible_WOS_DOI"] = ""
                df_work.at[idx, "Check_Title_OK"] = str(best_title_ok)
                df_work.at[idx, "Check_Year_OK"] = str(best_year_ok)
                df_work.at[idx, "Check_ISSN_OK"] = "doi_round"
                df_work.at[idx, "Check_Biblio_OK"] = "doi_round"
                df_work.at[idx, "Check_Authors_OK"] = "doi_round"
                df_work.at[idx, "Check_HostISBN_OK"] = "doi_round"
                df_work.at[idx, "Check_BookISBN_OK"] = "doi_round"
                accepted_count += 1
                print(f" ✓✓✓ ACCEPT VERIFIED WOS UID={best_uid} via DOI")
            else:
                print(" No exact DOI-based UID acceptance in round 1")

            print(f" -> accepted so far: {accepted_count}/{MAX_ACCEPTED}")
            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"\n[ERROR] Unexpected failure on round1 index {idx}, PID={row.get('PID','?')}: {e}")
            time.sleep(SLEEP_SECONDS)
            continue

    # ---- ROUND 2: Title/year -> WoS UID ----
    remaining_mask = (df_work["Verified_WOS_UID"].str.strip() == "")
    round2_index = list(df_work[remaining_mask].index)
    print(f"Round 2 rows (still missing WOS UID): {len(round2_index)}")

    for idx in tqdm(round2_index, desc="Round 2 Title -> WoS"):
        if accepted_count >= MAX_ACCEPTED:
            print(f"\nReached MAX_ACCEPTED={MAX_ACCEPTED}, stopping early.")
            break

        try:
            row = df_work.loc[idx]
            pid = row["PID"].strip()
            title = row["Title"].strip()
            year_str = row["Year"].strip()
            diva_pubtype = (row.get("PublicationType", "") or "").strip()
            diva_cat = diva_pubtype_category(diva_pubtype)

            try:
                pub_year = int(year_str)
            except Exception:
                pub_year = None

            print(f"\n[ROUND2 {idx}] PID={pid} PubType={diva_pubtype} (cat={diva_cat})")
            print(f" Title: '{title}'")
            print(f" Year: {pub_year}")
            print(
                f" DiVA biblio: Vol={row.get('Volume','')} "
                f"Issue={row.get('Issue','')} "
                f"Start={row.get('StartPage','')} End={row.get('EndPage','')}"
            )

            print(" -> querying WoS by title/year...")

            try:
                query_used, candidates = search_wos_title(title, pub_year, max_results=WOS_LIMIT)
            except Exception as e:
                print(f" ERROR querying WoS: {e}")
                time.sleep(SLEEP_SECONDS)
                continue

            df_work.at[idx, "WOS_Query"] = query_used
            df_work.at[idx, "WOS_Match_Method"] = "title_round"

            if not candidates or pub_year is None:
                print(" No candidates found or no valid year")
                time.sleep(SLEEP_SECONDS)
                continue

            cand_sims: list[tuple[str, float, int | None, str]] = []

            best_verified_uid = None
            best_verified_doi = None
            best_verified_score = 0.0
            best_possible_uid = None
            best_possible_doi = None
            best_possible_score = 0.0
            best_year_verified = None
            best_year_possible = None
            best_possible_checks = {}
            best_verified_checks = {}

            for hit in candidates:
                uid = extract_wos_uid(hit)
                cand_title = extract_wos_title(hit)
                cand_year = extract_wos_year(hit)
                cand_doi = extract_wos_doi(hit)
                cr_type = hit.get("documentType") or hit.get("doctype") or hit.get("types")

                print(f" cand: '{cand_title}' (WoS year={cand_year}, type={cr_type}, uid={uid})")
                if cand_year != pub_year:
                    print(" -> skip (year mismatch)")
                    continue

                wos_cat = wos_document_type_category(cr_type)
                if diva_cat and wos_cat and wos_cat != diva_cat:
                    print(f" -> skip (type mismatch: DiVA={diva_cat}, WoS={wos_cat})")
                    continue

                sim = title_similarity(title, cand_title)
                print(f" DOI: {cand_doi}")
                print(f" Title sim={sim:.3f}")

                cand_sims.append((uid, sim, cand_year, cand_doi))

                if sim < SIM_THRESHOLD:
                    print(f" -> skip (similarity {sim:.3f} < {SIM_THRESHOLD})")
                    continue

                if sim > best_possible_score:
                    best_possible_score = sim
                    best_possible_uid = uid
                    best_possible_doi = cand_doi
                    best_year_possible = cand_year

                print(" -> Title similarity OK, checking for VERIFICATION...")

                full_metadata = get_wos_full_metadata(uid) if uid else (hit or {})
                if not full_metadata:
                    print(" ⚠ Could not fetch full metadata, cannot verify")
                    continue

                wos_biblio = extract_wos_biblio(full_metadata)

                if diva_cat == "article":
                    need_issn = True
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = False
                    need_book_isbn = False
                elif diva_cat == "conference":
                    need_issn = False
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = True
                    need_book_isbn = False
                elif diva_cat == "chapter":
                    need_issn = False
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = True
                    need_book_isbn = False
                elif diva_cat == "book":
                    need_issn = False
                    need_biblio = False
                    need_authors = True
                    need_host_isbn = False
                    need_book_isbn = True
                else:
                    need_issn = False
                    need_biblio = True
                    need_authors = True
                    need_host_isbn = False
                    need_book_isbn = False

                issn_ok = True
                biblio_ok = True
                author_ok = True
                host_isbn_ok = True
                book_isbn_ok = True

                if need_issn:
                    issn_ok = issn_match(row, wos_biblio)

                if need_biblio:
                    biblio_ok = bibliographic_match(row, wos_biblio)

                if need_authors:
                    author_ok = authors_match(row, full_metadata)

                if need_host_isbn:
                    host_isbns = extract_host_isbns(row)
                    wos_isbns = extract_wos_isbns(full_metadata)
                    inter = host_isbns & wos_isbns
                    print(f" Host ISBNs (DiVA): {sorted(host_isbns)}")
                    print(f" WoS ISBNs: {sorted(wos_isbns)}")
                    print(f" Host ISBN intersection: {sorted(inter)}")
                    host_isbn_ok = bool(inter)

                if need_book_isbn:
                    book_isbns = extract_diva_book_isbns(row)
                    wos_isbns = extract_wos_isbns(full_metadata)
                    inter = book_isbns & wos_isbns
                    print(f" Book ISBNs (DiVA): {sorted(book_isbns)}")
                    print(f" WoS ISBNs: {sorted(wos_isbns)}")
                    print(f" Book ISBN intersection: {sorted(inter)}")
                    book_isbn_ok = bool(inter)

                all_ok = (
                    issn_ok
                    and biblio_ok
                    and (not need_authors or author_ok)
                    and (not need_host_isbn or host_isbn_ok)
                    and (not need_book_isbn or book_isbn_ok)
                )

                if all_ok:
                    print(" ✓✓✓ VERIFIED match (all required checks passed)")
                    if sim > best_verified_score:
                        best_verified_score = sim
                        best_verified_uid = uid
                        best_verified_doi = cand_doi
                        best_year_verified = cand_year
                        best_verified_checks = {
                            "Check_ISSN_OK": str(issn_ok),
                            "Check_Biblio_OK": str(biblio_ok),
                            "Check_Authors_OK": str(author_ok),
                            "Check_HostISBN_OK": str(host_isbn_ok),
                            "Check_BookISBN_OK": str(book_isbn_ok),
                            "Check_Category": diva_cat or "",
                            "Check_Title_OK": str(sim >= SIM_THRESHOLD),
                            "Check_Year_OK": str(cand_year == pub_year),
                        }
                else:
                    print(" ✗ Not all verification checks passed")
                    if sim == best_possible_score and best_possible_uid == uid:
                        best_possible_checks = {
                            "Check_ISSN_OK": str(issn_ok),
                            "Check_Biblio_OK": str(biblio_ok),
                            "Check_Authors_OK": str(author_ok),
                            "Check_HostISBN_OK": str(host_isbn_ok),
                            "Check_BookISBN_OK": str(book_isbn_ok),
                            "Check_Category": diva_cat or "",
                            "Check_Title_OK": str(sim >= SIM_THRESHOLD),
                            "Check_Year_OK": str(cand_year == pub_year),
                        }

            if best_verified_uid:
                df_work.at[idx, "Verified_WOS_UID"] = best_verified_uid
                df_work.at[idx, "Verified_WOS_DOI"] = best_verified_doi
                df_work.at[idx, "Possible_WOS_UID"] = ""
                df_work.at[idx, "Possible_WOS_DOI"] = ""
                for k, v in best_verified_checks.items():
                    df_work.at[idx, k] = v
                accepted_count += 1
                print(
                    f" ✓✓✓ ACCEPT VERIFIED WOS UID={best_verified_uid} "
                    f"(sim={best_verified_score:.3f}, year={best_year_verified})"
                )

            elif best_possible_uid:
                for k, v in best_possible_checks.items():
                    df_work.at[idx, k] = v
                df_work.at[idx, "Possible_WOS_UID"] = best_possible_uid
                df_work.at[idx, "Possible_WOS_DOI"] = best_possible_doi
                df_work.at[idx, "Verified_WOS_UID"] = ""
                df_work.at[idx, "Verified_WOS_DOI"] = ""
                accepted_count += 1
                print(
                    f" ✓ ACCEPT POSSIBLE WOS UID={best_possible_uid} "
                    f"(sim={best_possible_score:.3f}, year={best_year_possible})"
                )

            else:
                exact_matches = [(u, s, y, d) for (u, s, y, d) in cand_sims if s == 1.0]
                if exact_matches:
                    uid, s, y, doi = exact_matches[0]
                    df_work.at[idx, "Possible_WOS_UID"] = uid
                    df_work.at[idx, "Possible_WOS_DOI"] = doi
                    df_work.at[idx, "Verified_WOS_UID"] = ""
                    df_work.at[idx, "Verified_WOS_DOI"] = ""
                    df_work.at[idx, "Check_ISSN_OK"] = "title_only"
                    df_work.at[idx, "Check_Biblio_OK"] = "title_only"
                    df_work.at[idx, "Check_Authors_OK"] = "title_only"
                    df_work.at[idx, "Check_HostISBN_OK"] = "title_only"
                    df_work.at[idx, "Check_BookISBN_OK"] = "title_only"
                    df_work.at[idx, "Check_Category"] = diva_cat or ""
                    df_work.at[idx, "Check_Title_OK"] = "title_only"
                    df_work.at[idx, "Check_Year_OK"] = str(y == pub_year)
                    accepted_count += 1
                    print(f" ✓ FALLBACK POSSIBLE WOS UID={uid} (perfect title match, year={y})")
                else:
                    print(" REJECT all candidates (no WOS UID passed the minimum checks)")

            print(f" -> accepted so far: {accepted_count}/{MAX_ACCEPTED}")
            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print(f"\n[ERROR] Unexpected failure on round2 index {idx}, PID={row.get('PID','?')}: {e}")
            time.sleep(SLEEP_SECONDS)
            continue

    mask_has_candidate = (
        df_work["Possible_WOS_UID"].str.strip() != ""
    ) | (
        df_work["Verified_WOS_UID"].str.strip() != ""
    )
    df_out = df_work[mask_has_candidate].copy()

    csv_col_order = [
        "PID",
        "Verified_WOS_UID",
        "Possible_WOS_UID",
        "Verified_WOS_DOI",
        "Possible_WOS_DOI",
        "WOS_Match_Method",
        "WOS_Query",
        "Check_Category",
        "Check_Title_OK",
        "Check_Year_OK",
        "Check_ISSN_OK",
        "Check_Biblio_OK",
        "Check_Authors_OK",
        "Check_HostISBN_OK",
        "Check_BookISBN_OK",
        "DOI",
        "ISI",
        "ScopusId",
        "PMID",
        "Title",
        "Year",
        "PublicationType",
        "Journal",
        "Volume",
        "Issue",
        "Pages",
        "StartPage",
        "EndPage",
        "JournalISSN",
        "JournalEISSN",
        "SeriesISSN",
        "SeriesEISSN",
        "ISBN",
        "ISBN_PRINT",
        "ISBN_ELECTRONIC",
        "ISBN_UNDEFINED",
        "ArticleId",
        "Name",
        "Notes",
    ]
    csv_col_order = [c for c in csv_col_order if c in df_out.columns]
    remaining = [c for c in df_out.columns if c not in csv_col_order]
    csv_col_order.extend(remaining)
    df_out = df_out[csv_col_order]

    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"\nAccepted {accepted_count} records.")
    print(f"Wrote {len(df_out)} rows with candidates to {OUTPUT_CSV}")

    df_links = df_out.copy()
    df_links["PID_link"] = df_links["PID"].apply(make_pid_url)
    df_links["Verified_WOS_UID_link"] = df_links["Verified_WOS_UID"].apply(make_wos_uid_url)
    df_links["Possible_WOS_UID_link"] = df_links["Possible_WOS_UID"].apply(make_wos_uid_url)
    df_links["Verified_WOS_DOI_link"] = df_links["Verified_WOS_DOI"].apply(make_doi_url)
    df_links["Possible_WOS_DOI_link"] = df_links["Possible_WOS_DOI"].apply(make_doi_url)

    excel_col_order = [
        "PID",
        "PID_link",
        "Verified_WOS_UID",
        "Verified_WOS_UID_link",
        "Possible_WOS_UID",
        "Possible_WOS_UID_link",
        "Verified_WOS_DOI",
        "Verified_WOS_DOI_link",
        "Possible_WOS_DOI",
        "Possible_WOS_DOI_link",
        "WOS_Match_Method",
        "WOS_Query",
        "Check_Category",
        "Check_Title_OK",
        "Check_Year_OK",
        "Check_ISSN_OK",
        "Check_Biblio_OK",
        "Check_Authors_OK",
        "Check_HostISBN_OK",
        "Check_BookISBN_OK",
        "DOI",
        "ISI",
        "ScopusId",
        "PMID",
        "Title",
        "Year",
        "PublicationType",
        "Journal",
        "Volume",
        "Issue",
        "Pages",
        "StartPage",
        "EndPage",
        "JournalISSN",
        "JournalEISSN",
        "SeriesISSN",
        "SeriesEISSN",
        "ISBN",
        "ISBN_PRINT",
        "ISBN_ELECTRONIC",
        "ISBN_UNDEFINED",
        "ArticleId",
        "Name",
        "Notes",
    ]
    excel_col_order = [c for c in excel_col_order if c in df_links.columns]
    remaining = [c for c in df_links.columns if c not in excel_col_order]
    excel_col_order.extend(remaining)
    df_links = df_links[excel_col_order]

    with pd.ExcelWriter(EXCEL_OUT, engine="xlsxwriter") as writer:
        df_links.to_excel(writer, index=False, sheet_name="WOS UID candidates")
        ws = writer.sheets["WOS UID candidates"]

        header = list(df_links.columns)
        col_idx = {name: i for i, name in enumerate(header)}

        for row_xl, df_idx in enumerate(df_links.index, start=1):
            if df_links.at[df_idx, "PID_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["PID_link"],
                    df_links.at[df_idx, "PID_link"],
                    string="PID",
                )
            if df_links.at[df_idx, "Verified_WOS_UID_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Verified_WOS_UID_link"],
                    df_links.at[df_idx, "Verified_WOS_UID_link"],
                    string="Verified WOS UID",
                )
            if df_links.at[df_idx, "Possible_WOS_UID_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Possible_WOS_UID_link"],
                    df_links.at[df_idx, "Possible_WOS_UID_link"],
                    string="Possible WOS UID",
                )
            if df_links.at[df_idx, "Verified_WOS_DOI_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Verified_WOS_DOI_link"],
                    df_links.at[df_idx, "Verified_WOS_DOI_link"],
                    string="Verified DOI",
                )
            if df_links.at[df_idx, "Possible_WOS_DOI_link"]:
                ws.write_url(
                    row_xl,
                    col_idx["Possible_WOS_DOI_link"],
                    df_links.at[df_idx, "Possible_WOS_DOI_link"],
                    string="Possible DOI",
                )

    print(f"Wrote Excel with links to {EXCEL_OUT}")


if __name__ == "__main__":
    main()
