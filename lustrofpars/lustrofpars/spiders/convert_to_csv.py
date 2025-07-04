import json
import csv
from pathlib import Path
from tqdm import tqdm

def main():
    # Путь к JSON файлу
    json_file = Path('interier_products.json')
    csv_file = Path('interier_products.csv')

    if not json_file.exists():
        print("Файл JSON не найден!")
        return

    # Читаем данные из JSON
    with open(json_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Конвертируем в CSV
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'name', 'code', 'price', 'old_price', 'availability', 'url', 'category'
        ])
        writer.writeheader()

        for line in tqdm(lines, desc="Конвертация в CSV"):
            try:
                item = json.loads(line)
                # Заменяем None на пустые строки
                cleaned_item = {k: v if v is not None else '' for k, v in item.items()}
                writer.writerow(cleaned_item)
            except json.JSONDecodeError:
                continue

    print(f"Успешно конвертировано в {csv_file}")

if __name__ == "__main__":
    main()