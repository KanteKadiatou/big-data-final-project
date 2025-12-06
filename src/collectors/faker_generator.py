import os
import pandas as pd
from faker import Faker
import random

# -----------------------------P
# CONFIGURATION
# -----------------------------
RAW_SIMULATED_DIR = "data/raw/simulated"
OUTPUT_FILE = f"{RAW_SIMULATED_DIR}/students_synthetic.csv"

NUM_RECORDS = 500  # nombre d'élèves simulés

fake = Faker()

# -----------------------------
# FONCTIONS
# -----------------------------
def generate_student_record():
    """Génère un élève fictif avec des informations éducatives."""
    return {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "age": random.randint(15, 25),
        "gender": random.choice(["Male", "Female"]),
        "course": random.choice(["Math", "Physics", "Biology", "Computer Science", "AI"]),
        "score": round(random.uniform(0, 20), 2),
        "hours_studied": round(random.uniform(0, 40), 1),
        "assignments_completed": random.randint(0, 10),
        "participation": random.randint(0, 100)  # en pourcentage
    }

def generate_dataset(num_records=NUM_RECORDS):
    """Génère un dataset complet d'élèves."""
    if not os.path.exists(RAW_SIMULATED_DIR):
        os.makedirs(RAW_SIMULATED_DIR)

    data = [generate_student_record() for _ in range(num_records)]
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"✔ Dataset simulé généré : {OUTPUT_FILE} ({num_records} lignes)")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    generate_dataset()
