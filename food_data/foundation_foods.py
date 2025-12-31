import csv
import os

base_dir = os.path.join(os.path.dirname(__file__), 'usda', 'csv')
foundation_food_path = os.path.join(base_dir, 'foundation_food.csv')
food_path = os.path.join(base_dir, 'food.csv')

# Load all foundation fdc_ids
foundation_fdc_ids = set()
with open(foundation_food_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        foundation_fdc_ids.add(row['fdc_id'])

# Print fdc_id and description for each foundation food
with open(food_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['fdc_id'] in foundation_fdc_ids:
            print(f"{row['fdc_id']}: {row['description']}")