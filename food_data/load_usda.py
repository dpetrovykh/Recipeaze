import sqlite3
import csv
import os

# -----------------------------
# CONFIG
# -----------------------------
DB_FILE = "food_app.db"
CSV_FOLDER = "usda/csv"  # adjust path to where your CSVs are
ENABLE_FOREIGN_KEYS = True

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def create_tables(conn):
    cur = conn.cursor()
    # Layer 1 tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS food (
        food_id INTEGER PRIMARY KEY,
        description TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS nutrient (
        nutrient_id INTEGER PRIMARY KEY,
        name TEXT,
        unit TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS food_nutrient (
        food_id INTEGER,
        nutrient_id INTEGER,
        amount REAL,
        per_100g BOOLEAN DEFAULT 1,
        PRIMARY KEY (food_id, nutrient_id),
        FOREIGN KEY (food_id) REFERENCES food(food_id),
        FOREIGN KEY (nutrient_id) REFERENCES nutrient(nutrient_id)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS measure (
        measure_id INTEGER PRIMARY KEY,
        food_id INTEGER,
        label TEXT,
        grams REAL,
        FOREIGN KEY (food_id) REFERENCES food(food_id)
    )
    """)
    conn.commit()
    print("Layer 1 tables created successfully.")

def load_csv_to_table(conn, csv_filename, table_name, columns):
    csv_path = os.path.join(CSV_FOLDER, csv_filename)
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows_inserted = 0
        for row in reader:
            values = [row[col] for col in columns]
            placeholders = ",".join(["?"] * len(columns))
            conn.execute(
                f"INSERT OR IGNORE INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                values
            )
            rows_inserted += 1
        conn.commit()
        print(f"{rows_inserted} rows inserted into {table_name} from {csv_filename}.")

# -----------------------------
# MAIN
# -----------------------------
def main():
    conn = sqlite3.connect(DB_FILE)
    if ENABLE_FOREIGN_KEYS:
        conn.execute("PRAGMA foreign_keys = ON;")

    # 1️⃣ Create Layer 1 tables
    create_tables(conn)

    # 2️⃣ Load CSVs into tables
    load_csv_to_table(conn, "FOOD.csv", "food", ["food_id", "description"])
    load_csv_to_table(conn, "NUTRIENT.csv", "nutrient", ["nutrient_id", "name", "unit"])
    load_csv_to_table(conn, "FOOD_NUTRIENT.csv", "food_nutrient", ["food_id", "nutrient_id", "amount"])
    load_csv_to_table(conn, "MEASURE.csv", "measure", ["measure_id", "food_id", "label", "grams"])

    print("USDA Layer 1 data loaded successfully.")

    conn.close()

if __name__ == "__main__":
    main()
