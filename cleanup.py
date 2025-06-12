import os
import time
import threading
import logging
from config import TEMP_DIR, CLEANUP_INTERVAL, FILE_MAX_AGE

def cleanup_old_files():
    """Удаляет старые временные файлы."""
    while True:
        try:
            now = time.time()
            for filename in os.listdir(TEMP_DIR):
                filepath = os.path.join(TEMP_DIR, filename)
                if os.path.isfile(filepath) and (now - os.path.getmtime(filepath)) > FILE_MAX_AGE:
                    try:
                        os.remove(filepath)
                        logging.info(f"Удалён устаревший файл: {filename}")
                    except Exception as e:
                        logging.warning(f"Не удалось удалить файл {filename}: {e}")
            time.sleep(CLEANUP_INTERVAL)
        except Exception as e:
            logging.error(f"Ошибка при очистке файлов: {e}")
            time.sleep(60)

def start_cleanup_thread():
    """Запускает фоновый процесс очистки."""
    thread = threading.Thread(target=cleanup_old_files, daemon=True)
    thread.start()
    logging.info("Фоновый процесс очистки запущен")