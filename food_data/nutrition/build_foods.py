# python
import os, sqlite3, yaml

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
DB_PATH = os.path.join(BASE_DIR, "food_data", "food_app.db")
MAPPING_PATH = os.path.join(os.path.dirname(__file__), "mappings", "canonical_foods.yaml")

def load_mapping():
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def ensure_nutrition_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nutrition_food (
            id TEXT PRIMARY KEY,
            usda_food_id INTEGER NOT NULL,
            name TEXT NOT NULL
        )
        """
    )

def build_foods(conn, mapping):
    cur = conn.cursor()
    ok = 0
    for cid, cfg in mapping.items():
        usda_id = cfg["usda_food_id"]
        name = cfg["name"]
        cur.execute(
            """INSERT INTO nutrition_food (id, usda_food_id, name)
               VALUES (?, ?, ?)
               ON CONFLICT(id) DO UPDATE
                 SET usda_food_id=excluded.usda_food_id,
                     name=excluded.name""",
            (cid, usda_id, name),
        )
        ok += 1
    conn.commit()
    print(f"Inserted/updated {ok} canonical foods")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_nutrition_table(conn)
    mapping = load_mapping()
    build_foods(conn, mapping)
    conn.close()

if __name__ == "__main__":
    main()