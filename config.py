import os
import tempfile
import secrets
import logging

# Настройки пути к временным файлам
TEMP_DIR = os.path.join(tempfile.gettempdir(), 'timeflow_app')
os.makedirs(TEMP_DIR, exist_ok=True)

# Секретный ключ приложения
SECRET_KEY = secrets.token_hex(24)

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Настройки очистки временных файлов
CLEANUP_INTERVAL = 3600  # секунд (1 час)
FILE_MAX_AGE = 3600      # секунд (1 час)

# Скрываем папку (только для Windows)
if os.name == 'nt':
    import ctypes
    ctypes.windll.kernel32.SetFileAttributesW(TEMP_DIR, 0x02)