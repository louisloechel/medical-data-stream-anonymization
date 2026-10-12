import os
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import json

# Initialize Faker for generating usernames
fake = Faker()

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

# Generate unique users with consistent userids
def generate_users(num_users=10):
    users = []
    for _ in range(num_users):
        user_id = "03" + str(uuid.uuid4().int)[:5]  # Unique user ID with length 7 and starting with 03
        username = fake.name()
        user_initials = username[0] + username.split()[-1][0]
        users.append({
            "userid": user_id,
            "username": username,
            "userinitials": user_initials.upper()
        })
    return users

# Generate synthetic data with multiple users for each timestamp
def generate_synthetic_data_for_user(user, num_timestamps=100, interval_minutes=10):
    start_date = datetime.now() - timedelta(days=1)  # Start date for entries
    user_data = []

    # decide randomly whether user is registered correctly or not [QA use case]
    # chance of user being registered correctly is 80%
    if random.random() < 0.2:
        user["username"] = "Unknown"
        user["userinitials"] = "XX"

    # generate a starting ICD10 code for the user
    icd10 = generate_icd10_label()

    # Generate entries for each timestamp for this user
    for timestamp_index in range(num_timestamps):
        current_time = start_date + timedelta(minutes=interval_minutes * timestamp_index)
        entry_date = current_time.strftime("%d-%b")
        entry_time = current_time.strftime("%H:%M")

        # randomly change the ICD10 code for this user
        if random.random() < 0.1:  # 10% chance to change
            icd10 = generate_icd10_label()

        entry = {
            "recordid": str(uuid.uuid4().int)[:5],  # Unique entry identifier
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

        user_data.append(entry)
    
    return user_data

# Main function to create output folder and save individual JSON files
def generate_and_save_user_data(num_users, num_timestamps, interval_minutes):
    # Create output folder
    output_folder = "output_json_files"
    os.makedirs(output_folder, exist_ok=True)

    # Generate unique users
    users = generate_users(num_users)

    # Generate data and save a JSON file for each user
    for user in users:
        user_data = generate_synthetic_data_for_user(user, num_timestamps, interval_minutes)
        file_path = os.path.join(output_folder, f"{user['username'].replace(' ', '_')}_{user['userid']}.json")
        
        with open(file_path, "w") as json_file:
            json.dump(user_data, json_file, indent=2)
        print(f"Data saved for user {user['username']} with userid {user['userid']} at {file_path}")

# Run the function
generate_and_save_user_data(num_users=20, num_timestamps=10000, interval_minutes=10)