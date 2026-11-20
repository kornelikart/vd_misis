"""Load the Olist snapshots and Nager.Date, then build the local dashboard."""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
# Reserve the final seven archival days because the recorded flow declines sharply at the boundary.
START, END = date(2017, 1, 1), date(2018, 8, 24)
DAY = timedelta(days=1)
HORIZON = 14
CAPACITY = {"SP": 1800, "SE": 500, "OTHER": 650}
LABEL = {"SP": "Сан-Паулу", "SE": "Остальной Юго-Восток", "OTHER": "Другие штаты"}
URLS = {
    "olist.zip": "https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce",
    **{f"br-{year}.json": f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"
       for year in range(START.year, (END + HORIZON * DAY).year + 1)},
}
CSV_KEYS = {
    "orders": ("order_id",),
    "order_items": ("order_id", "order_item_id"),
    "sellers": ("seller_id",),
}


def days(first, last):
    return (first + i * DAY for i in range((last - first).days + 1))


def atomic_write(path, data):
    # Callers write while holding the database's writer lock.
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def fetch(url):
    for attempt in range(3):
        try:
            with urlopen(Request(url, headers={"User-Agent": "WarehouseAnalytics/1.0"}), timeout=25) as response:
                return response.read()
        except (URLError, TimeoutError) as error:
            if isinstance(error, HTTPError) and error.code not in (408, 429, 500, 502, 503, 504):
                raise
            if attempt == 2:
                raise
            delay = error.headers.get("Retry-After", "") if isinstance(error, HTTPError) else ""
            if delay.isdigit() and int(delay) > 30:
                raise
            time.sleep(max(2 ** attempt, int(delay) if delay.isdigit() else 0))


def landing(conn, name, data, quality):
    parsed = {}
    if name == "olist.zip":
        with ZipFile(io.BytesIO(data)) as archive:
            for kind, keys in CSV_KEYS.items():
                with archive.open(f"olist_{kind}_dataset.csv") as member:
                    reader = csv.DictReader(io.TextIOWrapper(member, encoding="utf-8-sig"))
                    if not set(keys).issubset(reader.fieldnames or ()):
                        raise ValueError(f"Missing key columns: {kind}")
                    parsed[kind] = [
                        (json.dumps([row[key] for key in keys]), row)
                        for row in reader if all(row.get(key) for key in keys)
                    ]
                    quality[f"empty_key_{kind}"] = reader.line_num - 1 - len(parsed[kind])
    else:
        year = int(name[3:7])
        events = json.loads(data)
        if not isinstance(events, list) or not events:
            raise ValueError(f"Empty or invalid event feed: {name}")
        parsed["holidays"] = []
        for event in events:
            when = date.fromisoformat(event["date"])
            if (when.year != year or event["countryCode"] != "BR"
                    or not isinstance(event["localName"], str)
                    or not isinstance(event["global"], bool)
                    or not isinstance(event["types"], list)
                    or not all(isinstance(kind, str) for kind in event["types"])
                    or (not event["global"] and not isinstance(event.get("counties"), list))):
                raise ValueError(f"Invalid calendar event: {event}")
            key = json.dumps([event["date"], event["localName"], event.get("counties")], sort_keys=True)
            parsed["holidays"].append((key, event))
    for kind, rows in parsed.items():
        if not rows:
            raise ValueError(f"No rows in {kind}")
        conn.executemany(
            "INSERT OR IGNORE INTO raw VALUES (?, ?, ?)",
            ((kind, key, json.dumps(row, ensure_ascii=False)) for key, row in rows),
        )


def records(conn, source):
    return (json.loads(row[0]) for row in conn.execute(
        "SELECT payload FROM raw WHERE source = ? ORDER BY record_key", (source,)))


def build_series(conn):
    quality = Counter()
    orders = {}
    sellers = {}
    shipments, balance = Counter(), Counter()
    holidays = defaultdict(set)
    for row in records(conn, "orders"):
        if not row.get("order_delivered_carrier_date"):
            quality["orders_without_handoff"] += 1
            continue
        try:
            purchase = datetime.fromisoformat(row["order_purchase_timestamp"])
            shipped = datetime.fromisoformat(row["order_delivered_carrier_date"])
            if shipped < purchase:
                raise ValueError("Handoff before purchase")
        except (ValueError, TypeError, KeyError):
            quality["orders_invalid_time"] += 1
            continue
        orders[row["order_id"]] = purchase.date(), shipped.date()
    for row in records(conn, "sellers"):
        state = row.get("seller_state")
        if state and len(state) == 2 and state.isalpha() and state.isupper():
            sellers[row["seller_id"]] = "SP" if state == "SP" else (
                "SE" if state in ("RJ", "MG", "ES") else "OTHER")
    for row in records(conn, "order_items"):
        order = orders.get(row["order_id"])
        zone = sellers.get(row["seller_id"])
        if order is None or zone is None:
            quality["items_without_valid_join"] += 1
            continue
        purchase, shipped = order
        shipments[zone, shipped] += 1
        balance[zone, purchase] += 1
        balance[zone, shipped] -= 1
        quality["items_joined"] += 1
    for event in records(conn, "holidays"):
        if event["global"] and "Public" in event["types"]:
            holidays[event["date"]].add(event["localName"])
    if not shipments or not holidays:
        raise ValueError("No usable shipment or holiday records")
    series = []
    first = min(START, min(key[1] for key in balance))
    for zone, capacity in CAPACITY.items():
        occupied = 0
        for day in days(first, END):
            occupied += balance[zone, day]
            if occupied < 0:
                raise ValueError("Negative cell usage")
            if day >= START:
                iso = day.isoformat()
                series.append((zone, iso, shipments[zone, day], occupied, capacity,
                               " / ".join(sorted(holidays[iso]))))
    if sum(row[2] for row in series) != sum(
            value for (_, day), value in shipments.items() if START <= day <= END):
        raise ValueError("Shipment reconciliation failed")
    conn.execute("DELETE FROM daily")
    conn.executemany("INSERT INTO daily VALUES (?, ?, ?, ?, ?, ?)", series)
    return series, quality, holidays


def forecast_and_test(series, holidays):
    data = {zone: {} for zone in CAPACITY}
    for zone, iso, shipments, occupied, *_ in series:
        data[zone][date.fromisoformat(iso)] = (shipments, occupied)
    results, predicted = [], []
    test_start = END - (4 * HORIZON - 1) * DAY
    for zone, history in data.items():
        absolute_error = [[0, 0], [0, 0]]
        for origin in days(test_start - DAY, END - HORIZON * DAY):
            if (origin - (test_start - DAY)).days % HORIZON:
                continue
            for target in days(origin + DAY, origin + HORIZON * DAY):
                recent = weekday_history(history, origin, target)
                for metric, actual in enumerate(history[target]):
                    for method, prediction in enumerate((sum(v[metric] for v in recent) / 4, recent[0][metric])):
                        absolute_error[metric][method] += abs(actual - prediction)
        for metric, errors in zip(("shipments", "occupied"), absolute_error):
            results.append({"zone": zone, "metric": metric, "mean4": round(errors[0] / 56, 2),
                            "last": round(errors[1] / 56, 2)})
        for target in days(END + DAY, END + HORIZON * DAY):
            recent = weekday_history(history, END, target)
            values = [round(sum(row[metric] for row in recent) / len(recent), 2)
                      for metric in range(2)]
            iso = target.isoformat()
            predicted.append((zone, iso, *values, CAPACITY[zone], " / ".join(sorted(holidays[iso]))))
    return predicted, results, test_start.isoformat()


def weekday_history(history, origin, target):
    last = origin - ((origin.weekday() - target.weekday()) % 7) * DAY
    return [history[last - i * 7 * DAY] for i in range(4)]


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run(offline, refresh_olist):
    DATA.mkdir(exist_ok=True)
    with sqlite3.connect(DATA / "warehouse.sqlite", timeout=120) as conn:
        conn.executescript("""
          CREATE TABLE IF NOT EXISTS raw(source TEXT, record_key TEXT, payload TEXT,
              PRIMARY KEY(source, record_key));
          CREATE TABLE IF NOT EXISTS snapshots(name TEXT, sha256 TEXT, PRIMARY KEY(name, sha256));
          CREATE TABLE IF NOT EXISTS daily(zone TEXT, day TEXT, shipments INTEGER,
              occupied INTEGER, capacity INTEGER, holiday TEXT, PRIMARY KEY(zone, day));
          CREATE TABLE IF NOT EXISTS forecast(zone TEXT, day TEXT, shipments REAL,
              occupied REAL, capacity INTEGER, holiday TEXT, PRIMARY KEY(zone, day));
          CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY, started TEXT, finished TEXT,
              status TEXT, inserted INTEGER, details TEXT);
        """)
        started = timestamp()
        inserted = 0
        notes = {}
        quality = Counter()
        try:
            conn.execute("BEGIN IMMEDIATE")
            for name, url in URLS.items():
                path = DATA / name
                status = "локальная копия"
                # The Kaggle dataset is a fixed historical release.
                if offline or (name == "olist.zip" and path.exists() and not refresh_olist):
                    payload = path.read_bytes()
                else:
                    try:
                        payload = fetch(url)
                        status = "получено по HTTPS"
                    except (URLError, TimeoutError):
                        if not path.exists():
                            raise
                        payload = path.read_bytes()
                        status = "резервная копия после сетевого сбоя"
                digest = hashlib.sha256(payload).hexdigest()
                known = conn.execute("SELECT 1 FROM snapshots WHERE name=? AND sha256=?",
                                     (name, digest)).fetchone()
                before = conn.total_changes
                if not known:
                    landing(conn, name, payload, quality)
                new = conn.total_changes - before
                if not known:
                    conn.execute("INSERT INTO snapshots VALUES (?, ?)", (name, digest))
                if status == "получено по HTTPS":
                    atomic_write(path, payload)
                notes[name] = {"status": status, "new": new, "sha256": digest}
                inserted += new
            series, checks, holidays = build_series(conn)
            future, metrics, test_start = forecast_and_test(series, holidays)
            conn.execute("DELETE FROM forecast")
            conn.executemany("INSERT INTO forecast VALUES (?, ?, ?, ?, ?, ?)", future)
            output = {"rows": series, "forecast": future, "metrics": metrics, "zones": LABEL,
                      "quality": dict(checks), "testStart": test_start}
            template = (ROOT / "dashboard.template.html").read_text(encoding="utf-8")
            embedded = json.dumps(output, ensure_ascii=False).replace("<", "\\u003c")
            page = template.replace("/*__DATA__*/null", embedded)
            status = "fallback" if any("сбоя" in v["status"] for v in notes.values()) else "ok"
            conn.execute("INSERT INTO runs VALUES (NULL, ?, ?, ?, ?, ?)",
                         (started, timestamp(), status, inserted, json.dumps(
                             {"sources": notes, "input_checks": quality, "data_checks": checks},
                             ensure_ascii=False)))
            atomic_write(ROOT / "dashboard.html", page.encode())
            conn.commit()
        except Exception as error:
            conn.rollback()
            conn.execute("INSERT INTO runs VALUES (NULL, ?, ?, 'error', 0, ?)",
                         (started, timestamp(), str(error)))
            conn.commit()
            raise
    print(f"{status}: {inserted} new rows, {len(series)} daily rows, {len(future)} forecasts")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--offline", action="store_true", help="Read packaged source snapshots")
    mode.add_argument("--refresh-olist", action="store_true", help="Check both remote sources")
    mode.add_argument("--journal", action="store_true", help="Show the last 10 pipeline runs")
    args = parser.parse_args()
    try:
        if args.journal:
            database = (DATA / "warehouse.sqlite").as_uri()
            with sqlite3.connect(database + "?mode=ro", uri=True) as conn:
                for row in conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 10"):
                    print(*row, sep=" | ")
        else:
            run(args.offline, args.refresh_olist)
    except (OSError, ValueError, KeyError, sqlite3.Error) as error:
        sys.exit(str(error))

