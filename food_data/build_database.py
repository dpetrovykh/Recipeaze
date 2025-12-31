# python
import os
import subprocess
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
DB_PATH = os.path.join(BASE_DIR, "food_data", "food_app.db")
SCRIPTS = [
    os.path.join(BASE_DIR, "food_data", "usda", "load_usda.py"),
    os.path.join(BASE_DIR, "food_data", "nutrition", "build_foods.py"),
    os.path.join(BASE_DIR, "food_data", "nutrition", "build_food_macros.py"),
]

def remove_existing_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed existing database at {DB_PATH}")

def run_step(script_path):
    name = os.path.basename(script_path)
    print(f"=== Running {name} ===")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.returncode != 0:
        print(f"[FAILED] {name} (exit {result.returncode})")
        if result.stderr:
            print(result.stderr.strip())
        sys.exit(result.returncode)
    print(f"[OK] {name}\n")

def main():
    remove_existing_db()
    for script in SCRIPTS:
        run_step(script)
    print("All steps completed successfully.")

if __name__ == "__main__":
    main()