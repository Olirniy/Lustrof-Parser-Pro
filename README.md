
# 🚀 Lustrof-Parser-Pro: Парсер каталога светильников

**Профессиональное решение для сбора данных интерьерных светильников с Lustrof.ru**

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![Scrapy](https://img.shields.io/badge/Scrapy-2.13-green)](https://scrapy.org)
[![License](https://img.shields.io/badge/License-MIT-red)](LICENSE)

## 🔍 Особенности
- **Полный каталог**: Сбор 50k+ товаров (название, цена, наличие, артикул).
- **Умный парсинг**: Обход антибот-систем, кеширование, дедупликация.
- **Гибкий экспорт**: CSV, JSON, XLSX с поддержкой обновлений.
- **Мониторинг**: Логирование прогресса, статистика в реальном времени.

## ⚙️ Технологии
```python
# Основной стек
Python >= 3.12
Scrapy >= 2.13
TQDM (визуализация)

# Оптимизация
- HTTP Cache (7 дней)
- AutoThrottle (адаптивная скорость)
- Rotating User-Agents
```

## 🛠 Установка
```bash
git clone https://github.com/ваш-аккаунт/Lustrof-Parser-Pro.git
cd Lustrof-Parser-Pro
pip install -r requirements.txt
```

## 🚦 Запуск
```bash
# Стандартный режим (с кешем)
scrapy crawl lustrofnewpars -O output/data_%(time)s.json

# Только обновления
scrapy crawl lustrofnewpars -a update_mode=True
```

## 📊 Пример данных
| Название          | Артикул | Цена  | Наличие       | Категория               |
|-------------------|---------|-------|---------------|-------------------------|
| Люстра Crystal    | 12345   | 8990  | В наличии     | Интерьерные светильники |
| Спот Galaxy       | 67890   | 1290  | Нет в наличии | Интерьерные светильники |

[Полный пример CSV](sample_interier_products.csv)

## 📜 Лицензия
MIT © 2025 Olirniy 
**Для коммерческого использования — согласование обязательно.**
```
