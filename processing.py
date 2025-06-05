# processing.py

import pandas as pd
import logging
from typing import List

def load_activity(df: pd.DataFrame, skill_groups: List[str]) -> pd.DataFrame:
    """Загружает и фильтрует активность по списку скилл-групп."""
    logging.info("Начало обработки активности...")
    if df.empty:
        raise ValueError("Файл активности пуст.")

    required = {
        'activity_date', 'start_time', 'end_time',
        'main_act', 'Основной функционал',
        'masterId', 'Скилл-группа'
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В активности отсутствуют: {missing}")

    # Проверяем формат даты в колонке activity_date, но позже будем использовать start
    date_format_pattern = r'(\d{2}\.\d{2}\.\d{4})|(\d{4}-\d{2}-\d{2})'
    if not df['activity_date'].astype(str).str.fullmatch(date_format_pattern).all():
        raise ValueError("Дата должна быть в формате DD.MM.YYYY или YYYY-MM-DD")

    df = df.copy()
    # Фильтруем по выбранным скилл-группам
    df['skill_lower'] = df['Скилл-группа'].astype(str).str.strip().str.lower()
    norm = [s.strip().lower() for s in skill_groups]
    df = df[df['skill_lower'].isin(norm)]
    if df.empty:
        raise ValueError("Нет данных для выбранных скилл-групп")

    # Заполняем столбцы с началом/концом события
    df['start'] = pd.to_datetime(df['activity_date'] + ' ' + df['start_time'], errors='coerce')
    df['end']   = pd.to_datetime(df['activity_date'] + ' ' + df['end_time'], errors='coerce')
    if df['start'].isna().all() or df['end'].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в активности")

    df = df.dropna(subset=['start', 'end'])
    df.loc[df['end'] <= df['start'], 'end'] += pd.Timedelta(days=1)

    df['main_act_lower'] = df['main_act'].str.lower()
    df['func_lower']     = df['Основной функционал'].str.lower()

    logging.info(f"Обработка активности завершена. Найдено: {len(df)} записей")
    return df

def extract_unique_skills(df: pd.DataFrame) -> list:
    """Извлекает уникальные скилл-группы из файла активности."""
    if df.empty or 'Скилл-группа' not in df.columns:
        return []
    skills = df['Скилл-группа'].astype(str).unique()
    skills = [s.strip() for s in skills if s.strip() and s.strip().lower() != 'nan']
    skills = sorted(set(skills))
    return skills

def load_slots(df: pd.DataFrame) -> pd.DataFrame:
    """Загрузка и обработка слотов (30-минутные интервалы + дельта)."""
    logging.info("Начало обработки слотов...")
    if df.empty:
        raise ValueError("Файл слотов пуст.")

    required = {'Дата', 'Время', 'Дельта'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В слотах отсутствуют: {missing}")

    if not df['Время'].astype(str).str.match(r'^\d{2}:\d{2}$').all():
        raise ValueError("Время должно быть в формате HH:MM")

    df = df.copy()
    df['Delta'] = df['Дельта'].astype(str).str.replace(',', '.').astype(float)
    if df['Delta'].isna().any():
        raise ValueError("Не удалось преобразовать значения дельты в числа")

    df['slot_start'] = pd.to_datetime(df['Дата'].astype(str) + ' ' + df['Время'].astype(str), errors='coerce')
    if df['slot_start'].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в слотах")

    df['slot_end'] = df['slot_start'] + pd.Timedelta(minutes=30)
    df['delta_min'] = df['Delta'] * 60

    logging.info(f"Обработка слотов завершена. Найдено: {len(df)} слотов")
    return df[['slot_start', 'slot_end', 'delta_min']]

def assign_calls(
    df_act: pd.DataFrame,
    df_slots: pd.DataFrame,
    min_interval: int,
    strategy: str,
    partial_coverage: bool,
    mass_activity: str
) -> pd.DataFrame:
    """
    Основная функция назначения.

    Параметры:
    - df_act: DataFrame активности (обязателен)
    - df_slots: DataFrame слотов, нужен только при strategy='by_delta'
    - min_interval: мин. длительность (в минутах)
    - strategy: 'by_delta' или 'mass'
    - partial_coverage: True/False
    - mass_activity: 'Входящие звонки' или 'Чат' (для mass)
    """
    assignments = []
    id_map = {}
    current_id = 1

    # МАССОВОЕ НАЗНАЧЕНИЕ
    if strategy == 'mass':
        df_mass = df_act[df_act['func_lower'].str.contains('omni', na=False)].copy()
        if mass_activity == 'Входящие звонки':
            src_mask = df_mass['main_act_lower'].str.contains('чат', na=False)
        else:
            src_mask = df_mass['main_act_lower'].str.contains('входящие звонки', na=False)

        df_mass = df_mass[src_mask]
        if df_mass.empty:
            return pd.DataFrame(columns=[
                'task_id','masterId','date_start','date_end','date_choice',
                'Категория активности','Назначенная активность',
                'description','education_program','time_choice',
                'slot_start','slot_end','назначено минут'
            ])

        for _, row in df_mass.iterrows():
            mid = row['masterId']
            date_str = row['start'].strftime('%d.%m.%Y')
            key = (mid, date_str)
            if key not in id_map:
                id_map[key] = current_id
                current_id += 1
            duration_min = int((row['end'] - row['start']).total_seconds() / 60)
            assignments.append({
                'task_id': id_map[key],
                'masterId': mid,
                'date_start': date_str,
                'date_end': date_str,
                'date_choice': 'Равномерно',
                'Категория активности': 'Работа на линии',
                'Назначенная активность': mass_activity,
                'description': '',
                'education_program': '',
                'time_choice': 'Интервал',
                'slot_start': row['start'].strftime('%H:%M:%S'),
                'slot_end': row['end'].strftime('%H:%M:%S'),
                'назначено минут': duration_min
            })
        return pd.DataFrame(assignments)

    # --- СТРАТЕГИЯ «ПОД ДЕЛЬТУ» ---
    df_act = df_act[df_act['func_lower'].str.contains('omni', na=False)]
    for _, slot in df_slots.iterrows():
        s0, e0, delta = slot['slot_start'], slot['slot_end'], slot['delta_min']
        slot_len = 30  # 30 минут

        if delta < 0:
            needed = abs(delta)
            src_mask = df_act['main_act_lower'].str.contains('чат', na=False)
            target_activity = 'Входящие звонки'
        else:
            needed = delta
            src_mask = df_act['main_act_lower'].str.contains('входящие звонки', na=False)
            target_activity = 'Чат'

        df_tmp = df_act[src_mask].copy()
        if df_tmp.empty:
            continue

        df_tmp['overlap'] = df_tmp.apply(
            lambda r: max(0, (min(r['end'], e0) - max(r['start'], s0)).total_seconds() / 60),
            axis=1
        )

        candidates = df_tmp[df_tmp['overlap'] >= min_interval].copy()
        if candidates.empty:
            if partial_coverage:
                candidates = df_tmp[df_tmp['overlap'] > 0].copy()
            else:
                continue

        unique_ids = list(candidates['masterId'].unique())

        full_slots = int(needed // slot_len)
        remainder = int(needed % slot_len)
        count_full = full_slots
        count_partial = 1 if (partial_coverage and remainder > 0) else 0
        total_needed_slots = count_full + count_partial
        assigned = 0

        for mid in unique_ids:
            if assigned >= total_needed_slots:
                break

            rec = candidates[candidates['masterId'] == mid].iloc[0]
            date_str = rec['start'].strftime('%d.%m.%Y')
            key = (mid, date_str)
            if key not in id_map:
                id_map[key] = current_id
                current_id += 1

            to_assign = slot_len
            if (partial_coverage and assigned == count_full and remainder > 0):
                to_assign = remainder

            assignments.append({
                'task_id': id_map[key],
                'masterId': mid,
                'date_start': date_str,
                'date_end': date_str,
                'date_choice': 'Равномерно',
                'Категория активности': 'Работа на линии',
                'Назначенная активность': target_activity,
                'description': '',
                'education_program': '',
                'time_choice': 'Интервал',
                'slot_start': s0.strftime('%H:%M:%S'),
                'slot_end': e0.strftime('%H:%M:%S'),
                'назначено минут': to_assign
            })

            assigned += 1

    if not assignments:
        return pd.DataFrame(columns=[
            'task_id','masterId','date_start','date_end','date_choice',
            'Категория активности','Назначенная активность',
            'description','education_program','time_choice',
            'slot_start','slot_end','назначено минут'
        ])

    df_out = pd.DataFrame(assignments)

    # Склеиваем подряд идущие интервалы
    df_out['slot_start_dt'] = pd.to_datetime(df_out['date_start'] + ' ' + df_out['slot_start'])
    df_out['slot_end_dt'] = pd.to_datetime(df_out['date_end'] + ' ' + df_out['slot_end'])

    df_out = df_out.sort_values(by=['masterId', 'date_start', 'slot_start_dt'])

    merged = []
    current = None

    for _, row in df_out.iterrows():
        if current is None:
            current = row
            continue

        same_person = row['masterId'] == current['masterId']
        same_date = row['date_start'] == current['date_start']
        same_activity = row['Назначенная активность'] == current['Назначенная активность']
        continuous = row['slot_start_dt'] == current['slot_end_dt']

        if same_person and same_date and same_activity and continuous:
            # Расширяем текущий интервал
            current['slot_end'] = row['slot_end']
            current['slot_end_dt'] = row['slot_end_dt']
            current['назначено минут'] += row['назначено минут']
        else:
            merged.append(current)
            current = row

    if current is not None:
        merged.append(current)

    df_merged = pd.DataFrame(merged)

    return df_merged[[
        'task_id','masterId','date_start','date_end','date_choice',
        'Категория активности','Назначенная активность',
        'description','education_program','time_choice',
        'slot_start','slot_end','назначено минут'
    ]]

    return pd.DataFrame(assignments)