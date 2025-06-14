import pandas as pd
import logging
from typing import List, Literal
from werkzeug.datastructures import FileStorage
from config import (
    COL_SKILL_GROUP, COL_TIME, COL_ASSIGNED_ACTIVITY, COL_CATEGORY,
    COL_ASSIGNED_MINUTES, COL_MAIN_ACTIVITY, COL_FUNC, COL_MASTER_ID,
    COL_ACTIVITY_DATE, COL_START_TIME, COL_END_TIME, COL_START, COL_END,
    COL_DELTA_MIN, COL_SLOT_START, COL_SLOT_END, COL_OVERLAP, COL_DATE_START,
    COL_DATE_END, COL_SLOT_START_DT, COL_SLOT_END_DT, VAL_OMNI, VAL_CHAT,
    VAL_CALLS, VAL_WORK_ON_LINE, VAL_UNIFORM, VAL_INTERVAL
)


def load_activity(file: FileStorage, skill_groups: List[str]) -> pd.DataFrame:
    """Загружает и фильтрует активность по списку скилл-групп."""
    logging.info("Начало обработки активности...")

    df = pd.read_excel(file)
    if df.empty:
        raise ValueError("Файл активности пуст.")

    required = {
        COL_ACTIVITY_DATE, COL_START_TIME, COL_END_TIME,
        COL_MAIN_ACTIVITY, COL_FUNC, COL_MASTER_ID, COL_SKILL_GROUP
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В активности отсутствуют: {missing}")

    date_format_pattern = r'(\d{2}\.\d{2}\.\d{4})|(\d{4}-\d{2}-\d{2})'
    if not df[COL_ACTIVITY_DATE].astype(str).str.fullmatch(date_format_pattern).all():
        raise ValueError("Дата должна быть в формате DD.MM.YYYY или YYYY-MM-DD")

    df = df.copy()
    df['skill_lower'] = df[COL_SKILL_GROUP].astype(str).str.strip().str.lower()
    norm = [s.strip().lower() for s in skill_groups]
    df = df[df['skill_lower'].isin(norm)]
    if df.empty:
        raise ValueError("Нет данных для выбранных скилл-групп")

    df[COL_START] = pd.to_datetime(
        df[COL_ACTIVITY_DATE].astype(str) + ' ' + df[COL_START_TIME].astype(str),
        format='mixed',
        dayfirst=True,
        errors='coerce'
    )
    df[COL_END] = pd.to_datetime(
        df[COL_ACTIVITY_DATE].astype(str) + ' ' + df[COL_END_TIME].astype(str),
        format='mixed',
        dayfirst=True,
        errors='coerce'
    )

    if df[COL_START].isna().all() or df[COL_END].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в активности")

    df = df.dropna(subset=[COL_START, COL_END])
    df.loc[df[COL_END] <= df[COL_START], COL_END] += pd.Timedelta(days=1)

    df[COL_START] = df[COL_START].dt.strftime('%Y-%m-%d %H:%M:%S')
    df[COL_END] = df[COL_END].dt.strftime('%Y-%m-%d %H:%M:%S')
    df[COL_START] = pd.to_datetime(df[COL_START])
    df[COL_END] = pd.to_datetime(df[COL_END])

    df['main_act_lower'] = df[COL_MAIN_ACTIVITY].str.lower()
    df['func_lower'] = df[COL_FUNC].str.lower()

    logging.info(f"Обработка активности завершена. Найдено: {len(df)} записей")
    return df


def extract_unique_skills(df: pd.DataFrame) -> List[str]:
    """Извлекает уникальные скилл-группы из файла активности."""
    if df.empty or COL_SKILL_GROUP not in df.columns:
        return []
    skills = df[COL_SKILL_GROUP].astype(str).unique()
    skills = [s.strip() for s in skills if s.strip() and s.strip().lower() != 'nan']
    return sorted(set(skills))


def load_slots(file: FileStorage) -> pd.DataFrame:
    """Загрузка и обработка слотов (30-минутные интервалы + дельта)."""
    logging.info("Начало обработки слотов...")

    df = pd.read_excel(file)
    if df.empty:
        raise ValueError("Файл слотов пуст.")

    required = {'Дата', 'Время', 'Дельта'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"В слотах отсутствуют: {missing}")

    if not df[COL_TIME].astype(str).str.match(r'^\d{2}:\d{2}$').all():
        raise ValueError("Время должно быть в формате HH:MM")

    df = df.copy()
    df['Delta'] = df['Дельта'].astype(str).str.replace(',', '.').astype(float)
    if df['Delta'].isna().any():
        raise ValueError("Не удалось преобразовать значения дельты в числа")

    df[COL_SLOT_START] = pd.to_datetime(
        df['Дата'].astype(str) + ' ' + df[COL_TIME].astype(str),
        format='mixed',
        dayfirst=True,
        errors='coerce'
    )

    if df[COL_SLOT_START].isna().all():
        raise ValueError("Не удалось распознать ни одну дату/время в слотах")

    df[COL_SLOT_END] = df[COL_SLOT_START] + pd.Timedelta(minutes=30)
    df[COL_DELTA_MIN] = df['Delta'] * 60

    df[COL_SLOT_START] = df[COL_SLOT_START].dt.strftime('%Y-%m-%d %H:%M:%S')
    df[COL_SLOT_END] = df[COL_SLOT_END].dt.strftime('%Y-%m-%d %H:%M:%S')
    df[COL_SLOT_START] = pd.to_datetime(df[COL_SLOT_START])
    df[COL_SLOT_END] = pd.to_datetime(df[COL_SLOT_END])

    logging.info(f"Обработка слотов завершена. Найдено: {len(df)} слотов")
    return df[[COL_SLOT_START, COL_SLOT_END, COL_DELTA_MIN]]


Strategy = Literal['by_delta', 'mass']
ActivityType = Literal['Входящие звонки', 'Чат']


def assign_calls(
    df_act: pd.DataFrame,
    df_slots: pd.DataFrame,
    min_interval: int,
    strategy: Strategy,
    partial_coverage: bool,
    mass_activity: ActivityType
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

    # --- МАССОВОЕ НАЗНАЧЕНИЕ ---
    if strategy == 'mass':
        df_mass = df_act[df_act['func_lower'].str.contains(VAL_OMNI, case=False, na=False)].copy()
        if df_mass.empty:
            logging.warning("Нет записей с VAL_OMNI для массового назначения")
            return pd.DataFrame()

        if mass_activity == VAL_CALLS:
            src_mask = df_mass['main_act_lower'].str.contains(VAL_CHAT, case=False, na=False)
        else:
            src_mask = df_mass['main_act_lower'].str.contains(VAL_CALLS, case=False, na=False)

        df_mass = df_mass[src_mask]
        if df_mass.empty:
            logging.warning("Нет подходящих записей для массового назначения")
            return pd.DataFrame()

        for _, row in df_mass.iterrows():
            mid = row[COL_MASTER_ID]
            date_str = row[COL_START].strftime('%Y-%m-%d')
            key = (mid, date_str)
            if key not in id_map:
                id_map[key] = current_id
                current_id += 1

            duration_min = int((row[COL_END] - row[COL_START]).total_seconds() / 60)
            assignments.append({
                'task_id': id_map[key],
                'masterId': mid,
                COL_DATE_START: date_str,
                COL_DATE_END: date_str,
                'date_choice': VAL_UNIFORM,
                COL_CATEGORY: VAL_WORK_ON_LINE,
                COL_ASSIGNED_ACTIVITY: mass_activity,
                'description': '',
                'education_program': '',
                'time_choice': VAL_INTERVAL,
                'slot_start': row[COL_START].strftime('%H:%M:%S'),
                'slot_end': row[COL_END].strftime('%H:%M:%S'),
                COL_ASSIGNED_MINUTES: duration_min
            })

        df_result = pd.DataFrame(assignments)
        if df_result.empty:
            logging.warning("Массовое назначение: не создано ни одной записи")
            return pd.DataFrame()

        logging.info(f"Массовое назначение: создано {len(df_result)} записей")
        return df_result

    # --- СТРАТЕГИЯ «ПОД ДЕЛЬТУ» ---
    df_act = df_act[df_act['func_lower'].str.contains(VAL_OMNI, case=False, na=False)]
    if df_act.empty:
        logging.warning("Нет записей с VAL_OMNI для стратегии 'by_delta'")
        return pd.DataFrame()

    if df_slots.empty:
        logging.warning("Нет слотов для стратегии 'by_delta'")
        return pd.DataFrame()

    for _, slot in df_slots.iterrows():
        s0, e0, delta = slot[COL_SLOT_START], slot[COL_SLOT_END], slot[COL_DELTA_MIN]

        if delta < 0:
            src_mask = df_act['main_act_lower'].str.contains(VAL_CHAT, case=False, na=False)
            target_activity = VAL_CALLS
        else:
            src_mask = df_act['main_act_lower'].str.contains(VAL_CALLS, case=False, na=False)
            target_activity = VAL_CHAT

        df_tmp = df_act[src_mask].copy()
        if df_tmp.empty:
            continue

        df_tmp[COL_OVERLAP] = df_tmp.apply(
            lambda r, s=s0, e=e0: max(0, (min(r[COL_END], e) - max(r[COL_START], s)).total_seconds() / 60),
            axis=1
        )

        candidates = df_tmp[df_tmp[COL_OVERLAP] >= min_interval].copy()
        if candidates.empty:
            if partial_coverage:
                candidates = df_tmp[df_tmp[COL_OVERLAP] > 0].copy()
            else:
                continue

        candidates = candidates.sort_values(by=COL_OVERLAP, ascending=False)
        unique_ids = list(candidates[COL_MASTER_ID].unique())

        full_slots = int(abs(delta) // 30)
        remainder = int(abs(delta) % 30)
        total_needed_slots = full_slots + (1 if (partial_coverage and remainder > 0) else 0)
        assigned = 0

        for mid in unique_ids:
            if assigned >= total_needed_slots:
                break

            rec = candidates[candidates[COL_MASTER_ID] == mid].iloc[0]
            date_str = rec[COL_START].strftime('%Y-%m-%d')
            key = (mid, date_str)
            if key not in id_map:
                id_map[key] = current_id
                current_id += 1

            to_assign = 30
            if partial_coverage and assigned == full_slots and remainder > 0:
                to_assign = remainder

            assignments.append({
                'task_id': id_map[key],
                'masterId': mid,
                COL_DATE_START: date_str,
                COL_DATE_END: date_str,
                'date_choice': VAL_UNIFORM,
                COL_CATEGORY: VAL_WORK_ON_LINE,
                COL_ASSIGNED_ACTIVITY: target_activity,
                'description': '',
                'education_program': '',
                'time_choice': VAL_INTERVAL,
                'slot_start': s0.strftime('%H:%M:%S'),
                'slot_end': e0.strftime('%H:%M:%S'),
                COL_ASSIGNED_MINUTES: to_assign
            })

            assigned += 1

    if not assignments:
        logging.warning("Нет подходящих записей для назначения")
        return pd.DataFrame()

    df_out = pd.DataFrame(assignments)

    # --- СКЛЕИВАНИЕ ИНТЕРВАЛОВ ---
    df_out[COL_SLOT_START_DT] = pd.to_datetime(
        df_out[COL_DATE_START] + ' ' + df_out['slot_start'],
        format='mixed',
        dayfirst=True,
        errors='coerce'
    )
    df_out[COL_SLOT_END_DT] = pd.to_datetime(
        df_out[COL_DATE_END] + ' ' + df_out['slot_end'],
        format='mixed',
        dayfirst=True,
        errors='coerce'
    )

    df_out = df_out.sort_values(by=['masterId', COL_DATE_START, COL_SLOT_START_DT])

    merged = []
    current = None

    for _, row in df_out.iterrows():
        if current is None:
            current = row
            continue

        same_person = row['masterId'] == current['masterId']
        same_date = row[COL_DATE_START] == current[COL_DATE_START]
        same_activity = row[COL_ASSIGNED_ACTIVITY] == current[COL_ASSIGNED_ACTIVITY]
        continuous = row[COL_SLOT_START_DT] == current[COL_SLOT_END_DT]

        if same_person and same_date and same_activity and continuous:
            current['slot_end'] = row['slot_end']
            current[COL_SLOT_END_DT] = row[COL_SLOT_END_DT]
            current[COL_ASSIGNED_MINUTES] += row[COL_ASSIGNED_MINUTES]
        else:
            merged.append(current)
            current = row

    if current is not None:
        merged.append(current)

    df_merged = pd.DataFrame(merged)

    # --- ФИНАЛЬНАЯ ОЧИСТКА ФОРМАТА ---
    df_merged[COL_DATE_START] = pd.to_datetime(df_merged[COL_DATE_START]).dt.strftime('%d.%m.%Y')
    df_merged[COL_DATE_END] = pd.to_datetime(df_merged[COL_DATE_END]).dt.strftime('%d.%m.%Y')
    df_merged['slot_start'] = df_merged['slot_start'].str.split('.').str[0]
    df_merged['slot_end'] = df_merged['slot_end'].str.split('.').str[0]

    logging.info(f"Финальное назначение: {len(df_merged)} записей")
    return df_merged[[
        'task_id', 'masterId', COL_DATE_START, COL_DATE_END, 'date_choice',
        COL_CATEGORY, COL_ASSIGNED_ACTIVITY,
        'description', 'education_program', 'time_choice',
        'slot_start', 'slot_end', COL_ASSIGNED_MINUTES
    ]]