# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
DB_PATH = r'C:\Users\Коля\Desktop\hr_metrics.db'
PLOT_DIR = os.path.join(BASE_DIR, "plots")
os.makedirs(PLOT_DIR, exist_ok=True)

BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден! Проверьте .env файл")

YANDEX_API_KEY = os.getenv('YANDEX_API_KEY')
YANDEX_FOLDER_ID = os.getenv('YANDEX_FOLDER_ID')
if not YANDEX_API_KEY or not YANDEX_FOLDER_ID:
    raise ValueError("Yandex API ключи не найдены! Проверьте .env файл")

YANDEX_MODEL_NAME = "yandexgpt"

AI_TIMEOUT = 30

RISK_THRESHOLDS = {
    'HIGH_ATTRITION': 10,
    'MEDIUM_ATTRITION': 5,
    'HIGH_ZERO_FTE': 20,
    'MEDIUM_ZERO_FTE': 10,
    'LOW_ZERO_FTE': 5,
    'CRITICAL_UNDEFINED': 50,
    'HIGH_UNDEFINED': 30,
    'MEDIUM_UNDEFINED': 15,
    'HIGH_RECENT_HIRES': 20,
    'MEDIUM_RECENT_HIRES': 10
}

EXPERIENCE_ORDER = [
    '1 мес', '2 мес', '3 мес', 'до 1 года', 
    '1-2 года', '2-3 года', '3-5 лет', 'более 5 лет'
]

LOG_LEVEL = "INFO"
LOG_FILE = os.path.join(BASE_DIR, "logs", "hr_bot.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)