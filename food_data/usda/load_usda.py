# python
import sqlite3
import csv
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
DB_FILE = os.path.join(BASE_DIR, "food_data", "food_app.db")
CSV_FOLDER = os.path.join(BASE_DIR, "food_data", "usda", "csv")
ENABLE_FOREIGN_KEYS = True

def create_tables(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS food (
        fdc_id INTEGER PRIMARY KEY,
        description TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS nutrient (
        id INTEGER PRIMARY KEY,
        name TEXT,
        unit_name TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS food_nutrient (
        id INTEGER,
        fdc_id INTEGER,
        nutrient_id INTEGER,
        amount REAL,
        per_100g BOOLEAN DEFAULT 1,
        PRIMARY KEY (fdc_id, nutrient_id),
        FOREIGN KEY (fdc_id) REFERENCES food(fdc_id),
        FOREIGN KEY (nutrient_id) REFERENCES nutrient(id)
    )
    """)
    # foundation_food now only stores fdc_id and looks up description from food
    cur.execute("""
    CREATE TABLE IF NOT EXISTS foundation_food (
        fdc_id INTEGER PRIMARY KEY,
        description TEXT,
        FOREIGN KEY (fdc_id) REFERENCES food(fdc_id)
    )
    """)
    conn.commit()
    print("Layer 1 tables created successfully.")

def load_csv_to_table(conn, csv_filename, table_name, columns):
    csv_path = os.path.join(CSV_FOLDER, csv_filename)
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return
    with open(csv_path, newline="", encoding="utf-8") as f:
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

def load_food_nutrient(conn):
    csv_path = os.path.join(CSV_FOLDER, "food_nutrient.csv")
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cur = conn.cursor()
        ok, skipped = 0, 0
        for row in reader:
            fdc_id = row["fdc_id"]
            nutrient_id = row["nutrient_id"]
            cur.execute("SELECT 1 FROM food WHERE fdc_id = ? LIMIT 1", (fdc_id,))
            if not cur.fetchone():
                skipped += 1
                continue
            cur.execute("SELECT 1 FROM nutrient WHERE id = ? LIMIT 1", (nutrient_id,))
            if not cur.fetchone():
                skipped += 1
                continue
            cur.execute(
                """INSERT OR IGNORE INTO food_nutrient (id, fdc_id, nutrient_id, amount)
                   VALUES (?, ?, ?, ?)""",
                (row["id"], fdc_id, nutrient_id, row["amount"])
            )
            ok += 1
        conn.commit()
    print(f"Inserted {ok} rows into food_nutrient; skipped {skipped} missing parents.")

def load_foundation_food(conn):
    """
    Load foundation food fdc_ids, then populate descriptions from food table.
    """
    # 1) insert just the fdc_ids from foundation_food.csv
    load_csv_to_table(conn, "foundation_food.csv", "foundation_food", ["fdc_id"])

    # 2) backfill descriptions from food table
    cur = conn.cursor()
    cur.execute("""
        UPDATE foundation_food
        SET description = (
            SELECT food.description
            FROM food
            WHERE food.fdc_id = foundation_food.fdc_id
        )
        WHERE description IS NULL
    """)
    conn.commit()
    print("foundation_food descriptions populated from food table.")

def main():
    conn = sqlite3.connect(DB_FILE)
    if ENABLE_FOREIGN_KEYS:
        conn.execute("PRAGMA foreign_keys = ON;")

    create_tables(conn)
    load_csv_to_table(conn, "food.csv", "food", ["fdc_id", "description"])
    load_csv_to_table(conn, "nutrient.csv", "nutrient", ["id", "name", "unit_name"])
    load_food_nutrient(conn)
    load_foundation_food(conn)

    print("USDA Layer 1 data loaded successfully.")
    conn.close()

if __name__ == "__main__":
    main()