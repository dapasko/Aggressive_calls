# processing.py

import pandas as pd
import logging
from typing import List, Dict, Tuple

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

    # Проверяем формат даты (DD.MM.YYYY или YYYY-MM-DD)
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

    # Конструируем start/end
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

def merge_chat_intervals(df_act_emp: pd.DataFrame, s0: pd.Timestamp, e0: pd.Timestamp) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Собирает у одного masterId все отрезки “Чат” (main_act='Чат'), которые пересекаются
    с диапазоном [s0,e0], и сшивает их в непрерывные блоки, **не объединяя через Перерывы**.
    Возвращает список (start,end).
    """
    # Берём ровно те строки, у которых main_act='чат' и есть пересечение с [s0,e0]
    tmp = df_act_emp[
        (df_act_emp['main_act_lower'] == 'чат') &
        (df_act_emp['end'] > s0) &
        (df_act_emp['start'] < e0)
    ].copy()
    if tmp.empty:
        return []

    # Оставляем лишь часть интервала внутри [s0,e0]
    tmp['cut_start'] = tmp['start'].apply(lambda x: max(x, s0))
    tmp['cut_end']   = tmp['end'].apply(lambda x: min(x, e0))
    tmp = tmp.sort_values(by='cut_start')

    merged = []
    cur_start = tmp.iloc[0]['cut_start']
    cur_end   = tmp.iloc[0]['cut_end']

    for _, row in tmp.iloc[1:].iterrows():
        st = row['cut_start']
        en = row['cut_end']
        # Если стык (st <= cur_end), то смыкаем – значит между ними не было «Перерыва» > 0
        if st <= cur_end:
            cur_end = max(cur_end, en)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = st, en
    merged.append((cur_start, cur_end))
    return merged

def assign_calls(
    df_act: pd.DataFrame,
    df_slots: pd.DataFrame,
    min_interval: int,
    strategy: str,
    partial_coverage: bool,
    mass_activity: str
) -> pd.DataFrame:
    """
    Основная функция назначения с «склейкой» смежных слотов,
    но **не через Перерывы/Обед**. Если между двумя Chat‐фрагментами
    есть «Перерыв» → они считаются разными блоками и не склеиваются.
    """
    assignments = []
    id_map = {}
    current_id = 1

    # ------------------ МАССОВОЕ НАЗНАЧЕНИЕ ------------------
    if strategy == 'mass':
        if mass_activity == 'Входящие звонки':
            src_mask = df_act['main_act_lower'].str.contains('чат', na=False)
        else:  # mass_activity == 'Чат'
            src_mask = df_act['main_act_lower'].str.contains('входящие звонки', na=False)

        df_mass = df_act[src_mask].copy()
        if df_mass.empty:
            return pd.DataFrame(columns=[
                'task_id', 'masterId', 'date_start', 'date_end', 'date_choice',
                'Категория активности', 'Назначенная активность',
                'description', 'education_program', 'time_choice',
                'slot_start', 'slot_end', 'назначено минут'
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

    # -------------- СТРАТЕГИЯ «ПОД ДЕЛЬТУ» --------------
    df_act = df_act[df_act['func_lower'].str.contains('omni', na=False)]

    # --- Новый блок: группировка подряд идущих слотов в «супергруппы» ---
    def slot_target_sign(delta_value: float) -> int:
        return -1 if delta_value < 0 else 1

    slot_list = []
    for _, row in df_slots.iterrows():
        sign = slot_target_sign(row['delta_min'])
        slot_list.append({
            'slot_start': row['slot_start'],
            'slot_end':   row['slot_end'],
            'delta_min':  row['delta_min'],
            'sign':       sign
        })

    # Сортируем по slot_start
    slot_list = sorted(slot_list, key=lambda x: x['slot_start'])

    groups = []
    current_group = None
    for slot in slot_list:
        if current_group is None:
            current_group = {
                'group_start': slot['slot_start'],
                'group_end':   slot['slot_end'],
                'delta_total': abs(slot['delta_min']),
                'sign':        slot['sign'],
                'slots':       [slot]
            }
        else:
            prev_end = current_group['group_end']
            if slot['slot_start'] == prev_end and slot['sign'] == current_group['sign']:
                # Расширяем «супергруппу»
                current_group['group_end']   = slot['slot_end']
                current_group['delta_total'] += abs(slot['delta_min'])
                current_group['slots'].append(slot)
            else:
                groups.append(current_group)
                current_group = {
                    'group_start': slot['slot_start'],
                    'group_end':   slot['slot_end'],
                    'delta_total': abs(slot['delta_min']),
                    'sign':        slot['sign'],
                    'slots':       [slot]
                }
    if current_group:
        groups.append(current_group)
    # --- Конец блока группировки ---

    # Обрабатываем каждую «супергруппу» слотов
    for grp in groups:
        s0 = grp['group_start']
        e0 = grp['group_end']
        needed = grp['delta_total']  # например, 150 минут сразу
        sign = grp['sign']

        if sign < 0:
            src_mask = df_act['main_act_lower'].str.contains('чат', na=False)
            target_activity = 'Входящие звонки'
        else:
            src_mask = df_act['main_act_lower'].str.contains('входящие звонки', na=False)
            target_activity = 'Чат'

        df_tmp = df_act[src_mask].copy()
        if df_tmp.empty:
            continue

        # --- Пытаемся покрыть всю «супергруппу» одним сотрудником ---
        df_tmp['overlap_group'] = df_tmp.apply(
            lambda r: max(0, (min(r['end'], e0) - max(r['start'], s0)).total_seconds() / 60),
            axis=1
        )
        candidates_ids = df_tmp['masterId'].unique()
        one_hit = False

        for mid in candidates_ids:
            df_emp = df_tmp[df_tmp['masterId'] == mid].copy()
            if df_emp.empty:
                continue

            # Сливаем подряд идущие «Чат»‐интервалы внутри [s0,e0], но через «Перерыв» не склеиваем
            merged_intervals = merge_chat_intervals(df_emp, s0, e0)
            for (m_start, m_end) in merged_intervals:
                total_len = int((m_end - m_start).total_seconds() / 60)
                if total_len >= needed:
                    # Этот сотрудник может покрыть всю дельту одним куском
                    overlap_start = m_start
                    overlap_end   = m_start + pd.Timedelta(minutes=needed)
                    date_str      = overlap_start.strftime('%d.%m.%Y')

                    key = (mid, date_str)
                    if key not in id_map:
                        id_map[key] = current_id
                        current_id += 1

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
                        'slot_start': overlap_start.strftime('%H:%M:%S'),
                        'slot_end': overlap_end.strftime('%H:%M:%S'),
                        'назначено минут': needed
                    })
                    one_hit = True
                    break
            if one_hit:
                break

        if one_hit:
            # Переходим к следующей «супергруппе»
            continue

        # ----------------------------------------------
        # «FALLBACK»-режим: склейка подряд «full»-слотов, но через Перерывы не склеиваем
        # ----------------------------------------------
        slots = grp['slots']

        # Для каждого слота находим, кто его покрывает «full» (overlap ≥ min_interval)
        slot_full_cover: List[List[int]] = []
        for slot in slots:
            s1 = slot['slot_start']
            e1 = slot['slot_end']
            if sign < 0:
                mask1 = df_act['main_act_lower'].str.contains('чат', na=False)
                tgt1 = 'Входящие звонки'
            else:
                mask1 = df_act['main_act_lower'].str.contains('входящие звонки', na=False)
                tgt1 = 'Чат'

            df_tmp1 = df_act[mask1].copy()
            df_tmp1['overlap'] = df_tmp1.apply(
                lambda r: max(0, (min(r['end'], e1) - max(r['start'], s1)).total_seconds() / 60),
                axis=1
            )
            full_ids = df_tmp1[df_tmp1['overlap'] >= min_interval]['masterId'].unique().tolist()
            slot_full_cover.append(full_ids)

        # Составляем карту masterId -> булев список длины len(slots), True = покрывает «full»
        master_slots_map: Dict[int, List[bool]] = {}
        for idx, full_ids in enumerate(slot_full_cover):
            for mid in full_ids:
                if mid not in master_slots_map:
                    master_slots_map[mid] = [False] * len(slots)
                master_slots_map[mid][idx] = True

        used_slot_indices = set()
        # --------------- Склейка подряд «full»-слотов ---------------
        for mid, covers in master_slots_map.items():
            i = 0
            while i < len(covers):
                if not covers[i] or i in used_slot_indices:
                    i += 1
                    continue
                # Нашли последовательность подряд «full»-слотов, покрываемых mid
                start_idx = i
                while i < len(covers) and covers[i] and i not in used_slot_indices:
                    i += 1
                end_idx = i - 1  # включительно

                first_slot = slots[start_idx]
                last_slot  = slots[end_idx]
                block_s = first_slot['slot_start']
                block_e = last_slot['slot_end']

                # Собираем реальные Chat-интервалы для mid в [block_s, block_e],
                # **но через «Перерыв» не склеиваем**:
                df_emp_all = df_act[df_act['masterId'] == mid].copy()
                merged_emp = merge_chat_intervals(df_emp_all, block_s, block_e)
                if not merged_emp:
                    continue

                # Выбираем тот merged-блок с максимальной длиной
                best_block = max(merged_emp, key=lambda x: (x[1] - x[0]).total_seconds())
                overlap_start, overlap_end = best_block
                duration_min = int((overlap_end - overlap_start).total_seconds() / 60)
                date_str = overlap_start.strftime('%d.%m.%Y')

                key = (mid, date_str)
                if key not in id_map:
                    id_map[key] = current_id
                    current_id += 1

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
                    'slot_start': overlap_start.strftime('%H:%M:%S'),
                    'slot_end': overlap_end.strftime('%H:%M:%S'),
                    'назначено минут': duration_min
                })

                # Помечаем эти слоты, чтобы не назначать их снова
                for j in range(start_idx, end_idx + 1):
                    used_slot_indices.add(j)

        # --------------- Обработка остаточных слотов (best-fit) ---------------
        leftover_indices = [i for i in range(len(slots)) if i not in used_slot_indices]
        for idx in leftover_indices:
            slot = slots[idx]
            s1 = slot['slot_start']
            e1 = slot['slot_end']
            delta1 = slot['delta_min']
            slot_len = 30

            if sign < 0:
                mask1 = df_act['main_act_lower'].str.contains('чат', na=False)
                tgt1 = 'Входящие звонки'
            else:
                mask1 = df_act['main_act_lower'].str.contains('входящие звонки', na=False)
                tgt1 = 'Чат'

            df_tmp1 = df_act[mask1].copy()
            df_tmp1['overlap'] = df_tmp1.apply(
                lambda r: max(0, (min(r['end'], e1) - max(r['start'], s1)).total_seconds() / 60),
                axis=1
            )

            full_candidates = df_tmp1[df_tmp1['overlap'] >= min_interval].copy()
            partial_candidates = (
                df_tmp1[df_tmp1['overlap'] > 0].copy() if partial_coverage else pd.DataFrame(columns=df_tmp1.columns)
            )

            # Выдаём «full»-слот
            full_candidates.sort_values(by='overlap', ascending=False, inplace=True)
            assigned_full = 0
            full_needed = int(abs(delta1) // slot_len)

            for mid in full_candidates['masterId'].unique():
                if assigned_full >= full_needed:
                    break
                rec = full_candidates[full_candidates['masterId'] == mid].iloc[0]
                date_str = rec['start'].strftime('%d.%m.%Y')
                key = (mid, date_str)
                if key not in id_map:
                    id_map[key] = current_id
                    current_id += 1

                assignments.append({
                    'task_id': id_map[key],
                    'masterId': mid,
                    'date_start': date_str,
                    'date_end': date_str,
                    'date_choice': 'Равномерно',
                    'Категория активности': 'Работа на линии',
                    'Назначенная активность': tgt1,
                    'description': '',
                    'education_program': '',
                    'time_choice': 'Интервал',
                    'slot_start': s1.strftime('%H:%M:%S'),
                    'slot_end': e1.strftime('%H:%M:%S'),
                    'назначено минут': slot_len
                })
                assigned_full += 1

            # Остаток
            remainder = abs(delta1) - (assigned_full * slot_len)
            if remainder > 0 and partial_coverage and not partial_candidates.empty:
                best_fit = partial_candidates[partial_candidates['overlap'] >= remainder].copy()
                if not best_fit.empty:
                    best_fit['diff'] = (best_fit['overlap'] - remainder).abs()
                    best_fit.sort_values(by='diff', ascending=True, inplace=True)
                    rec = best_fit.iloc[0]
                else:
                    partial_candidates.sort_values(by='overlap', ascending=False, inplace=True)
                    rec = partial_candidates.iloc[0]

                mid = rec['masterId']
                date_str = rec['start'].strftime('%d.%m.%Y')
                key = (mid, date_str)
                if key not in id_map:
                    id_map[key] = current_id
                    current_id += 1

                assignments.append({
                    'task_id': id_map[key],
                    'masterId': mid,
                    'date_start': date_str,
                    'date_end': date_str,
                    'date_choice': 'Равномерно',
                    'Категория активности': 'Работа на линии',
                    'Назначенная активность': tgt1,
                    'description': '',
                    'education_program': '',
                    'time_choice': 'Интервал',
                    'slot_start': s1.strftime('%H:%M:%S'),
                    'slot_end': e1.strftime('%H:%M:%S'),
                    'назначено минут': int(remainder)
                })

    # Конец цикла по всем «супергруппам»
    return pd.DataFrame(assignments)
