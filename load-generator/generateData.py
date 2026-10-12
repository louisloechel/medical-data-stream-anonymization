"""Generate synthetic per-patient JSON files for the load generator.

All values are random; no real data is used.

Schemas:

``v1`` (default, unchanged): recordid, date, time, resp, bps, pulse, spo2, temp,
    userinitials, username, userid, icd10. Written to ``output_json_files/``.
``v2``: everything of v1 plus the optional ICU fields of the record schema v2
    (see the README section "Record schema"):
    caseid, measured_at, quasi-identifiers (ageBand, sex, postcode2, ward,
    icd10 as ICD-10-GM code), monitor categories (rhythm, ectopy, ectopyCount,
    sqi, ventMode) and NEWS2 inputs (o2, acvpu, rass, gcs, spo2Scale).
    Some values are JSON null, as in real data.

Examples::

    python3 generateData.py                       # v1, 20 patients x 10000 records
    python3 generateData.py --schema v2 --num-users 10 --num-timestamps 600 \\
        --interval-minutes 1 --seed 2 --output-dir output_json_files_v2
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from faker import Faker

# Initialize Faker for generating usernames
fake = Faker()


def random_digits(n):
    """n random decimal digits (as string); reproducible with --seed."""
    return str(uuid.UUID(int=random.getrandbits(128)).int)[:n]


# Define ranges for physiological parameters
def generate_respiratory_rate():
    return random.randint(10, 30)

def generate_oxygen_saturation():
    return random.randint(85, 100)

def generate_blood_pressure_systolic():
    return random.randint(90, 200)

def generate_blood_pressure_diastolic():
    return random.randint(60, 120)

def generate_pulse():
    return random.randint(50, 130)

def generate_body_temperature():
    return round(random.uniform(35.0, 40.0), 1)

def generate_newstotal():
    return random.randint(1, 10)

def generate_newsrepeat():
    return random.choice(["1 hour", "4 hours", "12 hours"])

def generate_acvpu():
    return random.choice(["Alert", "Confused", "Voice", "Pain", "Unresponsive"])

def generate_oxcode():
    return random.choice(["", "RM", "N", "V40", "H28"])

def generate_oxpercent():
    return random.choice(["", "28", "40", "60"])

def generate_oxflow():
    return random.choice(["", "10", "14", "20"])

def generate_escalation():
    return random.choice(["Yes", "No"])

# def generate_waveform_label():
#     return random.choice(["", "I", "II", "III", "aVR", "aVL", "aVF", "Pleth", "V1", "V2", "V3", "V4", "V5", "V6"]) # waveform labels found in example dataset

def generate_icd10_label():
    icd10_leaf_codes = [
        "A00.0", "A00.1", "A00.9",
        "A01.0", "A01.1", "A01.2", "A01.3", "A01.4",
        "A02.0", "A02.1", "A02.2", "A02.8", "A02.9",
        "A15.0", "A15.1", "A15.2", "A15.3", "A15.4", "A15.5", "A15.6", "A15.7", "A15.8", "A15.9",
        "A20.0", "A20.1", "A20.2", "A20.3", "A20.7", "A20.8", "A20.9",
        "A21.0", "A21.1", "A21.2", "A21.3", "A21.7", "A21.8", "A21.9",
        "A22.0", "A22.1", "A22.2", "A22.7", "A22.8", "A22.9",
        "A23.0", "A23.1", "A23.2", "A23.3", "A23.8", "A23.9",
        "A24.0", "A24.1", "A24.2", "A24.3", "A24.4",
        "A25.0", "A25.1", "A25.9",
        "A26.0", "A26.7", "A26.8", "A26.9",
        "A27.0", "A27.8", "A27.9",
        "A28.0", "A28.1", "A28.2", "A28.8", "A28.9"
    ]
    return random.choice(icd10_leaf_codes)


# ---------------------------------------------------------------------------
# Schema v2 (ICU fields). Generic, made-up values only.

# 10-year age bands (og-prink/src/main/resources/hierarchies/age_bands.yaml), weighted towards older patients
AGE_BANDS = ["0-9", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89", "90-99", "100+"]
AGE_WEIGHTS = [1, 1, 3, 4, 7, 12, 20, 24, 18, 6, 1]

# Generic ward names (not the codes of any real hospital)
WARDS = ["ICU-A", "ICU-B", "IMC-1"]

# ICD-10-GM codes typical for intensive care (codes of different length on purpose)
ICU_ICD10 = ["J96.00", "J96.01", "J96.10", "J96.11", "J18.9", "J80", "A41.9", "A41.51",
             "I21.4", "I50.13", "I46.0", "N17.9", "R57.0", "R65.1", "S06.5"]

# Standard Philips monitor rhythm labels (og-prink/src/main/resources/hierarchies/rhythm.yaml) with weights
RHYTHMS = [
    ("Sinus-Rhythmus", 50), ("Sinus-Tachykardie", 12), ("Sinus-Bradykardie", 5),
    ("Afib Rhythmus", 10), ("HF unregelmäßig", 3),
    ("SV-Rhythmus", 1), ("SV-Tachykardie", 1), ("SV-Bradykardie", 1),
    ("Bigeminus", 1), ("Trigeminus", 1), ("Vent-Rhythmus", 1),
    ("Vent Fib/Tachy", 0.2), ("Vent-Tachykardie", 0.3), ("Persistierende VT", 0.2), ("Asystolie", 0.1),
    ("Pacer-Rhythmus", 4),
    ("-?-", 2), ("Rhythmus unbekannt", 1), ("Keine EKG-Analyse", 1), ("EKG Lernphase", 0.5), ("Rhythmus Lernphase", 0.5),
]

ECTOPY = [("none", 70), ("isolated PVC", 15), ("multiform PVC", 4), ("PVC pair", 4), ("run PVCs", 2), ("PACs", 5)]
SQI = [("Good", 85), ("Fair", 10), ("Bad", 5)]
VENT_MODES = ["CPAP/ASB", "BIPAP", "PC-AC", "SIMV", "NIV"]
ACVPU = [("A", 85), ("C", 6), ("V", 5), ("P", 3), ("U", 1)]


def weighted(choices):
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


def maybe_null(value, probability=0.02):
    """Return None with the given probability (missing measurements in real data)."""
    return None if random.random() < probability else value


def generate_icu_patient():
    """Per-patient constants of schema v2."""
    icd10 = random.choice(ICU_ICD10)
    ventilated = random.random() < 0.4
    return {
        "caseid": "SYN" + random_digits(8),
        "ageBand": random.choices(AGE_BANDS, weights=AGE_WEIGHTS, k=1)[0],
        "sex": None if random.random() < 0.05 else random.choice(["M", "F"]),
        "postcode2": f"{random.randint(1, 99):02d}",
        "ward": random.choice(WARDS),
        "icd10": None if random.random() < 0.1 else icd10,
        # SpO2 scale 2 for hypercapnic respiratory failure (J96.x1), scale 1 otherwise
        "spo2Scale": 2 if icd10.startswith("J96.") and icd10.endswith("1") else 1,
        "ventilated": ventilated,
        "o2_propensity": 0.95 if ventilated else random.choice([0.1, 0.5, 0.8]),
    }


def generate_icu_fields(patient):
    """Per-record ICU fields of schema v2."""
    ventilated = patient["ventilated"]
    o2 = True if ventilated else random.random() < patient["o2_propensity"]
    ectopy = weighted(ECTOPY)
    if ventilated:
        rass = random.choice([-5, -4, -3, -2, -1, 0])
        gcs = random.randint(3, 11)
        acvpu = random.choice(["V", "P", "U"]) if random.random() < 0.7 else "A"
    else:
        rass = random.choices([-2, -1, 0, 1, 2], weights=[5, 15, 70, 7, 3], k=1)[0]
        gcs = random.choices([15, 14, 13, 12], weights=[80, 12, 5, 3], k=1)[0]
        acvpu = weighted(ACVPU)
    return {
        "rhythm": maybe_null(weighted(RHYTHMS)),
        "ectopy": maybe_null(ectopy),
        "ectopyCount": 0 if ectopy == "none" else random.randint(1, 30),
        "sqi": weighted(SQI),
        "ventMode": random.choice(VENT_MODES) if ventilated else None,
        "o2": maybe_null(o2),
        "acvpu": maybe_null(acvpu, 0.05),
        "rass": maybe_null(rass, 0.1),
        "gcs": maybe_null(gcs, 0.1),
        "spo2Scale": patient["spo2Scale"],
    }

# Generate unique users with consistent userids
def generate_users(num_users=10):
    users = []
    for _ in range(num_users):
        user_id = "03" + random_digits(5)  # Unique user ID with length 7 and starting with 03
        username = fake.name()
        user_initials = username[0] + username.split()[-1][0]
        users.append({
            "userid": user_id,
            "username": username,
            "userinitials": user_initials.upper()
        })
    return users

# Generate synthetic data with multiple users for each timestamp
def generate_synthetic_data_for_user(user, num_timestamps=100, interval_minutes=10, schema="v1"):
    start_date = datetime.now().replace(microsecond=0) - timedelta(days=1)  # Start date for entries (local time)
    user_data = []

    # decide randomly whether user is registered correctly or not [QA use case]
    # chance of user being registered correctly is 80%
    if random.random() < 0.2:
        user["username"] = "Unknown"
        user["userinitials"] = "XX"

    # generate a starting ICD10 code for the user
    icd10 = generate_icd10_label()
    patient = generate_icu_patient() if schema == "v2" else None

    # Generate entries for each timestamp for this user
    for timestamp_index in range(num_timestamps):
        current_time = start_date + timedelta(minutes=interval_minutes * timestamp_index)
        entry_date = current_time.strftime("%d-%b")
        entry_time = current_time.strftime("%H:%M")

        # randomly change the ICD10 code for this user
        if random.random() < 0.1:  # 10% chance to change
            icd10 = generate_icd10_label()

        entry = {
            "recordid": random_digits(5),  # Unique entry identifier
            "date": entry_date,
            "time": entry_time,
            "resp": generate_respiratory_rate(),
            "bps": generate_blood_pressure_systolic(),
            "pulse": generate_pulse(),
            "spo2": generate_oxygen_saturation(),
            "temp": generate_body_temperature(),
            "userinitials": user["userinitials"],
            "username": user["username"],           # firstname lastname
            "userid": user["userid"],
            "icd10": icd10,
        }

        # randomly invalidate single datapoints: 5% chance for username to be "Unknown"
        if random.random() < 0.05:
            entry["username"] = "Unknown"
            entry["userinitials"] = "XX"

        if patient is not None:
            entry["caseid"] = patient["caseid"]
            entry["measured_at"] = (current_time.astimezone(timezone.utc)
                                    .isoformat(timespec="milliseconds").replace("+00:00", "Z"))
            for field in ("ageBand", "sex", "postcode2", "ward", "icd10"):
                entry[field] = patient[field]
            entry.update(generate_icu_fields(patient))
            for vital in ("resp", "bps", "pulse", "spo2", "temp"):
                entry[vital] = maybe_null(entry[vital])

        user_data.append(entry)

    return user_data

# Main function to create output folder and save individual JSON files
def generate_and_save_user_data(num_users, num_timestamps, interval_minutes, schema="v1", output_folder="output_json_files"):
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)

    # Generate unique users
    users = generate_users(num_users)

    # Generate data and save a JSON file for each user
    for user in users:
        user_data = generate_synthetic_data_for_user(user, num_timestamps, interval_minutes, schema)
        file_path = os.path.join(output_folder, f"{user['username'].replace(' ', '_')}_{user['userid']}.json")

        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(user_data, json_file, indent=2, ensure_ascii=False)
        print(f"Data saved for user {user['username']} with userid {user['userid']} at {file_path}")


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate synthetic per-patient JSON files.")
    ap.add_argument("--schema", choices=("v1", "v2"), default="v1",
                    help="v1: legacy fields (default); v2: plus the ICU fields of record schema v2")
    ap.add_argument("--num-users", type=int, default=20)
    ap.add_argument("--num-timestamps", type=int, default=10000, help="records per patient")
    ap.add_argument("--interval-minutes", type=float, default=10)
    ap.add_argument("--output-dir", default=None,
                    help="default: output_json_files (v1) or output_json_files_v2 (v2)")
    ap.add_argument("--seed", type=int, default=None, help="seed for reproducible output")
    args = ap.parse_args(argv)
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
    output_dir = args.output_dir or ("output_json_files_v2" if args.schema == "v2" else "output_json_files")
    generate_and_save_user_data(args.num_users, args.num_timestamps, args.interval_minutes, args.schema, output_dir)


if __name__ == "__main__":
    main()
