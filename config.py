import os
import tempfile
import secrets
import logging
import ctypes

# --- Настройки пути к временным файлам ---
TEMP_DIR = os.path.join(tempfile.gettempdir(), 'timeflow_app')
os.makedirs(TEMP_DIR, exist_ok=True)

# --- Секретный ключ приложения ---
SECRET_KEY = secrets.token_hex(24)

# --- Настройки логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Настройки очистки временных файлов ---
CLEANUP_INTERVAL = 3600  # секунд (1 час)
FILE_MAX_AGE = 3600      # секунд (1 час)

# --- Скрываем папку (только для Windows) ---
if os.name == 'nt':
    ctypes.windll.kernel32.SetFileAttributesW(TEMP_DIR, 0x02)

# --- Константы для колонок ---
COL_SKILL_GROUP = 'Скилл-группа'
COL_TIME = 'Время'
COL_ASSIGNED_ACTIVITY = 'Назначенная активность'
COL_CATEGORY = 'Категория активности'
COL_ASSIGNED_MINUTES = 'назначено минут'
COL_MAIN_ACTIVITY = 'main_act'
COL_FUNC = 'Основной функционал'
COL_MASTER_ID = 'masterId'
COL_ACTIVITY_DATE = 'activity_date'
COL_START_TIME = 'start_time'
COL_END_TIME = 'end_time'
COL_START = 'start'
COL_END = 'end'
COL_DELTA_MIN = 'delta_min'
COL_SLOT_START = 'slot_start'
COL_SLOT_END = 'slot_end'
COL_OVERLAP = 'overlap'
COL_DATE_START = 'date_start'
COL_DATE_END = 'date_end'
COL_SLOT_START_DT = 'slot_start_dt'
COL_SLOT_END_DT = 'slot_end_dt'

# --- Константы для значений ---
VAL_OMNI = 'omni'
VAL_CHAT = 'Чат'
VAL_CALLS = 'Входящие звонки'
VAL_WORK_ON_LINE = 'Работа на линии'
VAL_UNIFORM = 'Равномерно'
VAL_INTERVAL = 'Интервал'