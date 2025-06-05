import pandas as pd
import logging
from typing import Tuple


def load_activity(df: pd.DataFrame, skill_group: str) -> pd.DataFrame:
    """Загружает и фильтрует активность по скилл-группе."""
    logging.info("Начало обработки активности...")

    if df.empty:
        raise ValueError("Файл активности пуст.")

    required = {'activity_date', 'start_time', 'end_time', 'main_act', 'Основной функционал', 'masterId',
                'Скилл-группа'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В активности отсутствуют: {missing}")

    date_format_pattern = r'(\d{2}\.\d{2}\.\d{4})|(\d{4}-\d{2}-\d{2})'
    if not df['activity_date'].astype(str).str.fullmatch(date_format_pattern).all():
        raise ValueError("Дата должна быть в формате DD.MM.YYYY или YYYY-MM-DD")

    df = df.copy()
    mask = df['Скилл-группа'].astype(str).str.strip().str.lower() == skill_group.strip().lower()
    df = df[mask]

    if df.empty:
        raise ValueError(f"Нет данных для Скилл-группы: '{skill_group}'")

    df['start'] = pd.to_datetime(df['activity_date'] + ' ' + df['start_time'], errors='coerce')
    df['end'] = pd.to_datetime(df['activity_date'] + ' ' + df['end_time'], errors='coerce')

    if df['start'].isna().all() or df['end'].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в активности")

    df = df.dropna(subset=['start', 'end'])
    df.loc[df['end'] <= df['start'], 'end'] += pd.Timedelta(days=1)

    df['main_act_lower'] = df['main_act'].str.lower()
    df['func_lower'] = df['Основной функционал'].str.lower()

    logging.info(f"Обработка активности завершена. Найдено: {len(df)} записей")
    return df


def extract_unique_skills(df: pd.DataFrame) -> list:
    """Извлекает уникальные скилл-группы из файла активности."""
    if df.empty:
        return []

    if 'Скилл-группа' not in df.columns:
        return []

    # Извлекаем уникальные значения, фильтруем пустые и преобразуем в список
    skills = df['Скилл-группа'].astype(str).unique()
    skills = [s.strip() for s in skills if s.strip() and s.strip().lower() != 'nan']
    skills = sorted(set(skills))  # Убираем дубликаты и сортируем

    return skills


# Остальные функции остаются без изменений, но нужно обновить load_activity:
def extract_unique_skills(df: pd.DataFrame) -> list:
    """Извлекает уникальные скилл-группы из файла активности."""
    if df.empty:
        return []

    if 'Скилл-группа' not in df.columns:
        return []

    # Извлекаем уникальные значения, фильтруем пустые и преобразуем в список
    skills = df['Скилл-группа'].astype(str).unique()
    skills = [s.strip() for s in skills if s.strip() and s.strip().lower() != 'nan']
    skills = sorted(set(skills))  # Убираем дубликаты и сортируем

    return skills


# Остальные функции остаются без изменений, но нужно обновить load_activity:
def load_activity(df: pd.DataFrame, skill_groups: list) -> pd.DataFrame:
    """Загружает и фильтрует активность по списку скилл-групп."""
    logging.info(f"Начало обработки активности для групп: {skill_groups}")

    if df.empty:
        raise ValueError("Файл активности пуст.")

    required = {'activity_date', 'start_time', 'end_time', 'main_act', 'Основной функционал', 'masterId',
                'Скилл-группа'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В активности отсутствуют: {missing}")

    date_format_pattern = r'(\d{2}\.\d{2}\.\d{4})|(\d{4}-\d{2}-\d{2})'
    if not df['activity_date'].astype(str).str.fullmatch(date_format_pattern).all():
        raise ValueError("Дата должна быть в формате DD.MM.YYYY или YYYY-MM-DD")

    df = df.copy()

    # Фильтрация по списку групп
    mask = df['Скилл-группа'].astype(str).str.strip().str.lower().isin(
        [sg.strip().lower() for sg in skill_groups]
    )
    df = df[mask]

    if df.empty:
        raise ValueError(f"Нет данных для выбранных скилл-групп: {skill_groups}")

    df['start'] = pd.to_datetime(df['activity_date'] + ' ' + df['start_time'], errors='coerce')
    df['end'] = pd.to_datetime(df['activity_date'] + ' ' + df['end_time'], errors='coerce')

    if df['start'].isna().all() or df['end'].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в активности")

    df = df.dropna(subset=['start', 'end'])
    df.loc[df['end'] <= df['start'], 'end'] += pd.Timedelta(days=1)

    df['main_act_lower'] = df['main_act'].str.lower()
    df['func_lower'] = df['Основной функционал'].str.lower()

    logging.info(f"Обработка активности завершена. Найдено: {len(df)} записей")
    return df

def load_slots(df: pd.DataFrame) -> pd.DataFrame:
    """Загрузка и обработка слотов."""
    logging.info("Начало обработки слотов...")

    if df.empty:
        raise ValueError("Файл слотов пуст.")

    required = {'Дата', 'Время', 'Дельта'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В слотах отсутствуют: {missing}")

    # Проверка формата времени
    if not df['Время'].astype(str).str.match(r'^\d{2}:\d{2}$').all():
        raise ValueError("Время должно быть в формате HH:MM")

    df = df.copy()
    df['Дельта'] = df['Дельта'].astype(str).str.replace(',', '.').astype(float)

    if df['Дельта'].isna().any():
        raise ValueError("Не удалось преобразовать значения Дельты в числа")

    df['slot_start'] = pd.to_datetime(df['Дата'].astype(str) + ' ' + df['Время'].astype(str), errors='coerce')
    if df['slot_start'].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в слотах")

    df['slot_end'] = df['slot_start'] + pd.Timedelta(minutes=30)
    df['delta_min'] = df['Дельта'] * 60

    logging.info(f"Обработка слотов завершена. Найдено: {len(df)} слотов")
    return df[['slot_start', 'slot_end', 'delta_min']]


def assign_calls(df_act: pd.DataFrame, df_slots: pd.DataFrame) -> pd.DataFrame:
    """Назначение активности на основе слотов."""
    assignments = []
    current_id = 1
    id_map = {}
    df_act = df_act[df_act['func_lower'].str.contains('omni', na=False)]

    for _, slot in df_slots.iterrows():
        s0, e0, delta = slot['slot_start'], slot['slot_end'], slot['delta_min']
        slot_len = 30

        if delta < 0:
            needed = abs(delta)
            mask = df_act['main_act_lower'].str.contains('чат', na=False)
            target = 'Входящие звонки'
        else:
            needed = delta
            mask = df_act['main_act_lower'].str.contains('входящие звонки', na=False)
            target = 'Чат'

        df_tmp = df_act[mask].copy()
        df_tmp['overlap'] = df_tmp.apply(
            lambda r: max(0, (min(r['end'], e0) - max(r['start'], s0)).total_seconds() / 60),
            axis=1
        )

        candidates = df_tmp[df_tmp['overlap'] >= slot_len]
        count = int((needed + slot_len - 1) // slot_len)
        selected = candidates['masterId'].unique()[:count]
        remaining = needed

        for mid in selected:
            match = df_tmp[(df_tmp['masterId'] == mid) & (df_tmp['overlap'] >= slot_len)]
            if match.empty:
                continue

            rec = match.iloc[0]
            date_assigned = pd.to_datetime(rec['activity_date']).strftime('%d.%m.%Y')
            key = (mid, date_assigned)

            if key not in id_map:
                id_map[key] = current_id
                current_id += 1

            assignments.append({
                'id': id_map[key],
                'masterId': mid,
                'date_start': date_assigned,
                'date_end': date_assigned,
                'date_choice': 'Равномерно',
                'Категория активности': 'Работа на линии',
                'Назначенная активность': target,
                'description': '',
                'education_program': '',
                'time_choice': 'Интервал',
                'slot_start': s0.time(),
                'slot_end': e0.time(),
                'назначено минут': min(slot_len, remaining)
            })

            remaining -= min(slot_len, remaining)
            if remaining <= 0:
                break

    return pd.DataFrame(assignments)

