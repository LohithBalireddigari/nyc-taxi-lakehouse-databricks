"""Synthetic NYC yellow-taxi data for Oct-Dec 2022 (TLC schema + ``driver_id``).

Standard library only, so it runs locally and on a cluster (writing to /dbfs).

Realism:
  * hourly / weekday demand curves, holiday dips, rush-hour slowdowns
  * zone popularity (Midtown, airports) and distance from rough zone coordinates
  * TLC fare rules: metered fare, JFK flat fare, tips mostly on card,
    congestion surcharge, airport fee, tolls on airport runs
  * two driver snapshots so the driver dimension has real SCD2 changes

Deliberately dirty rows (to exercise quality rules):
  negative fares, zero distance, dropoff before pickup, 0/7+ passengers,
  unknown zones (264/265), absurd distances, exact duplicate rows.
"""

import argparse
import csv
import json
import math
import os
import random
from datetime import date, datetime, timedelta

# (LocationID, Borough, Zone, service_zone, x_miles, y_miles, popularity)
ZONES = [
    (1, "EWR", "Newark Airport", "EWR", -12.0, -2.0, 1),
    (4, "Manhattan", "Alphabet City", "Yellow Zone", 1.6, 0.6, 3),
    (7, "Queens", "Astoria", "Boro Zone", 4.5, 5.5, 2),
    (13, "Manhattan", "Battery Park City", "Yellow Zone", -0.6, -1.4, 3),
    (43, "Manhattan", "Central Park", "Yellow Zone", 1.0, 5.0, 4),
    (48, "Manhattan", "Clinton East", "Yellow Zone", 0.2, 3.2, 6),
    (68, "Manhattan", "East Chelsea", "Yellow Zone", 0.3, 1.8, 6),
    (74, "Manhattan", "East Harlem North", "Boro Zone", 2.6, 8.0, 2),
    (79, "Manhattan", "East Village", "Yellow Zone", 1.2, 0.9, 7),
    (87, "Manhattan", "Financial District North", "Yellow Zone", 0.2, -1.0, 4),
    (90, "Manhattan", "Flatiron", "Yellow Zone", 0.7, 1.9, 6),
    (100, "Manhattan", "Garment District", "Yellow Zone", 0.6, 2.8, 6),
    (107, "Manhattan", "Gramercy", "Yellow Zone", 1.1, 1.8, 6),
    (113, "Manhattan", "Greenwich Village North", "Yellow Zone", 0.7, 1.1, 5),
    (114, "Manhattan", "Greenwich Village South", "Yellow Zone", 0.5, 0.8, 5),
    (132, "Queens", "JFK Airport", "Airports", 10.0, -4.0, 8),
    (138, "Queens", "LaGuardia Airport", "Airports", 6.5, 6.0, 7),
    (140, "Manhattan", "Lenox Hill East", "Yellow Zone", 2.0, 4.8, 5),
    (141, "Manhattan", "Lenox Hill West", "Yellow Zone", 1.6, 4.7, 6),
    (142, "Manhattan", "Lincoln Square East", "Yellow Zone", 0.6, 4.4, 7),
    (161, "Manhattan", "Midtown Center", "Yellow Zone", 1.0, 3.4, 10),
    (162, "Manhattan", "Midtown East", "Yellow Zone", 1.4, 3.3, 9),
    (163, "Manhattan", "Midtown North", "Yellow Zone", 0.8, 3.8, 8),
    (164, "Manhattan", "Midtown South", "Yellow Zone", 1.0, 2.7, 8),
    (170, "Manhattan", "Murray Hill", "Yellow Zone", 1.4, 2.6, 7),
    (181, "Brooklyn", "Park Slope", "Boro Zone", 1.5, -4.0, 2),
    (186, "Manhattan", "Penn Station/Madison Sq West", "Yellow Zone", 0.5, 2.6, 8),
    (230, "Manhattan", "Times Sq/Theatre District", "Yellow Zone", 0.6, 3.3, 9),
    (234, "Manhattan", "Union Sq", "Yellow Zone", 0.9, 1.5, 7),
    (236, "Manhattan", "Upper East Side North", "Yellow Zone", 2.2, 5.8, 8),
    (237, "Manhattan", "Upper East Side South", "Yellow Zone", 1.8, 4.6, 9),
    (238, "Manhattan", "Upper West Side North", "Yellow Zone", 1.0, 6.4, 6),
    (239, "Manhattan", "Upper West Side South", "Yellow Zone", 0.6, 5.2, 7),
    (249, "Manhattan", "West Village", "Yellow Zone", 0.1, 1.0, 6),
    (255, "Brooklyn", "Williamsburg (North Side)", "Boro Zone", 3.0, 0.5, 2),
    (261, "Manhattan", "World Trade Center", "Yellow Zone", -0.2, -0.8, 4),
]
UNKNOWN_ZONES = [(264, "Unknown", "NV", "N/A"), (265, "Unknown", "NA", "N/A")]

HOURLY = [0.9, 0.6, 0.4, 0.3, 0.3, 0.5, 1.2, 2.4, 3.2, 3.0, 2.6, 2.7, 2.9, 2.9, 3.0, 3.2, 3.4, 3.9, 4.3, 4.0, 3.5, 3.1, 2.6, 1.7]
WEEKDAY = [1.0, 1.05, 1.1, 1.15, 1.2, 1.1, 0.85]  # Mon..Sun
HOLIDAYS = {date(2022, 10, 10): 0.8, date(2022, 11, 11): 0.85, date(2022, 11, 24): 0.55, date(2022, 12, 25): 0.5,
            date(2022, 12, 26): 0.7, date(2022, 12, 31): 1.2}

FIRST = ["Ahmed", "Maria", "Jose", "Wei", "Ivan", "Priya", "Kwame", "Luis", "Fatima", "Dmitri", "Ana", "Rahul",
         "Chen", "Omar", "Grace", "Samuel", "Yusuf", "Elena", "Mohammed", "Sofia"]
LAST = ["Khan", "Rodriguez", "Singh", "Li", "Petrov", "Diallo", "Garcia", "Haque", "Nguyen", "Cohen", "Mensah",
        "Lopez", "Ali", "Kim", "Reddy", "Ortiz"]
BASES = ["Midtown Garage", "LIC Fleet", "Brooklyn Navy Yard", "Hunts Point Depot"]
VEHICLES = ["Toyota Camry Hybrid", "Ford Escape Hybrid", "Nissan NV200", "Toyota RAV4 Hybrid", "Tesla Model Y"]


def _distance(a, b, rng):
    if a[0] == b[0]:
        return round(rng.uniform(0.4, 1.6), 2)
    d = (abs(a[4] - b[4]) + abs(a[5] - b[5])) * 1.15
    return round(max(0.3, d * rng.uniform(0.9, 1.25)), 2)


def _speed_mph(hour, rng):
    base = 7.5 if hour in (8, 9, 16, 17, 18) else 11.0 if 7 <= hour <= 20 else 16.0
    return base * rng.uniform(0.75, 1.3)


def _fare(distance, minutes, pu, do):
    jfk_manhattan = (pu == 132 and do != 132) or (do == 132 and pu != 132)
    if jfk_manhattan:
        return 2, 52.0
    if pu == 1 or do == 1:
        return 3, round(2.5 + 2.5 * distance + 0.5 * minutes + 17.5, 2)
    return 1, round(2.5 + 2.5 * distance + 0.5 * minutes / 1.2, 2)


def _drivers(n, rng):
    rows = []
    for i in range(1, n + 1):
        rows.append([f"D{i:04d}", f"{rng.choice(FIRST)} {rng.choice(LAST)}", rng.choice(VEHICLES), rng.choice(BASES),
                     "ACTIVE", round(rng.uniform(4.2, 5.0), 2)])
    return rows


def generate(output_dir: str, n_trips: int = 60000, n_drivers: int = 400, seed: int = 7,
             start: date = date(2022, 10, 1), end: date = date(2022, 12, 31)) -> dict:
    rng = random.Random(seed)
    os.makedirs(output_dir, exist_ok=True)

    # ---- zone lookup ---------------------------------------------------
    zdir = os.path.join(output_dir, "zones")
    os.makedirs(zdir, exist_ok=True)
    with open(os.path.join(zdir, "taxi_zone_lookup.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        for z in ZONES:
            w.writerow(z[:4])
        w.writerows(UNKNOWN_ZONES)

    # ---- drivers: two snapshots (SCD2) ----------------------------------
    drivers = _drivers(n_drivers, rng)
    ddir = os.path.join(output_dir, "drivers")
    os.makedirs(ddir, exist_ok=True)
    header = ["driver_id", "driver_name", "vehicle_type", "base_name", "status", "rating", "snapshot_date"]
    with open(os.path.join(ddir, "drivers_2022-10-01.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for d in drivers:
            w.writerow(d + ["2022-10-01"])
    changes = 0
    with open(os.path.join(ddir, "drivers_2022-11-15.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for d in drivers:
            d = list(d)
            r = rng.random()
            if r < 0.06:
                d[2] = rng.choice([v for v in VEHICLES if v != d[2]])   # new vehicle
                changes += 1
            elif r < 0.10:
                d[3] = rng.choice([b for b in BASES if b != d[3]])      # moved base
                changes += 1
            elif r < 0.12:
                d[4] = "SUSPENDED"
                changes += 1
            d[5] = round(min(5.0, max(3.5, d[5] + rng.uniform(-0.2, 0.1))), 2)  # rating drift (not tracked)
            w.writerow(d + ["2022-11-15"])
        for i in range(n_drivers + 1, n_drivers + 31):                       # new hires
            w.writerow([f"D{i:04d}", f"{rng.choice(FIRST)} {rng.choice(LAST)}", rng.choice(VEHICLES),
                        rng.choice(BASES), "ACTIVE", round(rng.uniform(4.3, 5.0), 2), "2022-11-15"])
        w.writerow(["", "No Id", VEHICLES[0], BASES[0], "ACTIVE", 4.8, "2022-11-15"])  # bad row
        w.writerow(["D9999", "Bad Rating", VEHICLES[0], BASES[0], "ACTIVE", 7.5, "2022-11-15"])  # bad row

    # ---- trips ---------------------------------------------------------
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    day_w = [WEEKDAY[d.weekday()] * HOLIDAYS.get(d, 1.0) * (1.0 + 0.002 * i) for i, d in enumerate(days)]
    zone_w = [z[6] for z in ZONES]
    by_month = {}
    for _ in range(n_trips):
        day = rng.choices(days, weights=day_w)[0]
        hour = rng.choices(range(24), weights=HOURLY)[0]
        pickup = datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59))
        pu = rng.choices(ZONES, weights=zone_w)[0]
        do = rng.choices(ZONES, weights=zone_w)[0]
        dist = _distance(pu, do, rng)
        minutes = max(2.0, dist / _speed_mph(hour, rng) * 60 + rng.uniform(1, 4))
        dropoff = pickup + timedelta(minutes=minutes)
        ratecode, fare = _fare(dist, minutes, pu[0], do[0])
        payment = rng.choices([1, 2, 3, 4], weights=[76, 21, 2, 1])[0]
        tip = round(fare * rng.choice([0.15, 0.18, 0.2, 0.2, 0.25, 0.3, 0.1, 0.0]), 2) if payment == 1 else 0.0
        tolls = 6.55 if (pu[0] in (132, 138, 1) or do[0] in (132, 138, 1)) and rng.random() < 0.6 else 0.0
        extra = 1.0 if hour >= 20 or hour < 6 else (2.5 if 16 <= hour < 20 and day.weekday() < 5 else 0.0)
        congestion = 2.5 if pu[1] == "Manhattan" or do[1] == "Manhattan" else 0.0
        airport_fee = 1.25 if pu[0] in (132, 138) else 0.0
        total = round(fare + extra + 0.5 + tip + tolls + 0.3 + congestion + airport_fee, 2)
        passengers = rng.choices([1, 2, 3, 4, 5, 6], weights=[70, 15, 5, 3, 4, 3])[0]

        row = {
            "VendorID": rng.choice([1, 2, 2]),
            "tpep_pickup_datetime": pickup.strftime("%Y-%m-%d %H:%M:%S"),
            "tpep_dropoff_datetime": dropoff.strftime("%Y-%m-%d %H:%M:%S"),
            "passenger_count": passengers,
            "trip_distance": dist,
            "RatecodeID": ratecode,
            "store_and_fwd_flag": "Y" if rng.random() < 0.01 else "N",
            "PULocationID": pu[0],
            "DOLocationID": do[0],
            "payment_type": payment,
            "fare_amount": fare,
            "extra": extra,
            "mta_tax": 0.5,
            "tip_amount": tip,
            "tolls_amount": tolls,
            "improvement_surcharge": 0.3,
            "total_amount": total,
            "congestion_surcharge": congestion,
            "airport_fee": airport_fee,
            "driver_id": f"D{rng.randint(1, n_drivers):04d}",
        }

        # ---- inject dirty data --------------------------------------
        r = rng.random()
        if r < 0.004:
            row["fare_amount"], row["total_amount"] = -row["fare_amount"], -row["total_amount"]
        elif r < 0.012:
            row["trip_distance"] = 0.0
        elif r < 0.015:
            row["tpep_dropoff_datetime"] = (pickup - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        elif r < 0.025:
            row["passenger_count"] = 0 if rng.random() < 0.8 else ""
        elif r < 0.03:
            row["PULocationID"] = rng.choice([264, 265])
        elif r < 0.031:
            row["trip_distance"] = round(rng.uniform(300, 9000), 1)

        rows = by_month.setdefault(pickup.strftime("%Y-%m"), [])
        rows.append(row)
        if rng.random() < 0.004:  # exact duplicate (re-sent record)
            rows.append(dict(row))

    tdir = os.path.join(output_dir, "trips")
    os.makedirs(tdir, exist_ok=True)
    counts = {}
    for month, rows in sorted(by_month.items()):
        rows.sort(key=lambda r: r["tpep_pickup_datetime"])
        with open(os.path.join(tdir, f"yellow_tripdata_{month}.csv"), "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        counts[month] = len(rows)

    return {"trips_by_month": counts, "trips": sum(counts.values()), "zones": len(ZONES) + 2,
            "drivers": n_drivers, "driver_changes": changes}


def main() -> None:
    p = argparse.ArgumentParser(description="Generate synthetic NYC taxi data")
    p.add_argument("--output", default="data/raw")
    p.add_argument("--trips", type=int, default=60000)
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args()
    print(json.dumps(generate(a.output, n_trips=a.trips, seed=a.seed), indent=2))


if __name__ == "__main__":
    main()
