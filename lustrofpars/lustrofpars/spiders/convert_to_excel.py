import json
import csv
import re
from pathlib import Path
from tqdm import tqdm

def sanitize_text(text):
    """Очищает текст от спецсимволов, мешающих CSV"""
    if text is None:
        return ""
    return re.sub(r'[\n\r\t;"]', ' ', str(text))

def main():
    """Конвертирует JSON файл в CSV с правильными настройками для Excel"""
    json_file = Path('interier_products.json')
    csv_file = Path('interier_products.csv')

    if not json_file.exists():
        print("Файл JSON не найден!")
        return

    # Читаем данные из JSON
    with open(json_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Конвертируем в CSV с правильными настройками для Excel
    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:  # utf-8-sig добавляет BOM
        # Используем точку с запятой как разделитель для русской локализации
        writer = csv.DictWriter(
            f,
            fieldnames=['name', 'code', 'price', 'old_price', 'availability', 'url', 'category'],
            delimiter=';',
            quoting=csv.QUOTE_ALL  # Все поля в кавычках
        )

        writer.writeheader()

        for line in tqdm(lines, desc="Конвертация в CSV"):
            try:
                item = json.loads(line)

                # Очищаем все поля
                cleaned_item = {}
                for key, value in item.items():
                    cleaned_item[key] = sanitize_text(value)

                writer.writerow(cleaned_item)
            except json.JSONDecodeError:
                continue

    print(f"Файл {csv_file} готов для идеального открытия в Excel")

if __name__ == "__main__":
    main()