# python
import os, sqlite3, yaml

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
DB_PATH = os.path.join(BASE_DIR, "food_data", "food_app.db")
MAPPING_PATH = os.path.join(os.path.dirname(__file__), "mappings", "canonical_foods.yaml")

NUTRIENT_IDS = {
    "calories": 1008,
    "protein_g": 1003,
    "fat_g": 1004,
    "carbs_g": 1005,
}

def load_mapping():
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def ensure_macros_table(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nutrition_food_macros (
            id TEXT PRIMARY KEY,
            calories REAL,
            protein REAL,
            fat REAL,
            carbs REAL
        )
        """
    )

def fetch_amount(cur, usda_id, nutrient_id):
    row = cur.execute(
        "SELECT amount FROM food_nutrient WHERE fdc_id = ? AND nutrient_id = ? LIMIT 1",
        (usda_id, nutrient_id),
    ).fetchone()
    return row[0] if row else 0.0

def build_macros(conn):
    cur = conn.cursor()
    mapping = load_mapping()
    ok = 0
    for cid, cfg in mapping.items():
        usda_id = cfg["usda_food_id"]
        calories = fetch_amount(cur, usda_id, NUTRIENT_IDS["calories"])
        protein = fetch_amount(cur, usda_id, NUTRIENT_IDS["protein_g"])
        fat = fetch_amount(cur, usda_id, NUTRIENT_IDS["fat_g"])
        carbs = fetch_amount(cur, usda_id, NUTRIENT_IDS["carbs_g"])
        cur.execute(
            """INSERT INTO nutrition_food_macros (id, calories, protein, fat, carbs)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE
                 SET calories=excluded.calories,
                     protein=excluded.protein,
                     fat=excluded.fat,
                     carbs=excluded.carbs""",
            (cid, calories, protein, fat, carbs),
        )
        ok += 1
    conn.commit()
    print(f"Inserted/updated {ok} macro rows")

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_macros_table(conn)
    build_macros(conn)
    conn.close()

if __name__ == "__main__":
    main()