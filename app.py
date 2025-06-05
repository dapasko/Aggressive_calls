# app.py

from flask import (
    Flask, request, render_template, send_file,
    redirect, url_for, flash, session, jsonify
)
from processing import load_activity, load_slots, assign_calls, extract_unique_skills
from utils import save_temp_file, load_temp_file, generate_excel_buffer
from cleanup import start_cleanup_thread
from config import SECRET_KEY

import webbrowser
import threading
import pandas as pd
import logging

app = Flask(__name__)
app.secret_key = SECRET_KEY

@app.before_request
def before_request():
    session.permanent = False

@app.route('/', methods=['GET', 'POST'])
def index():
    table = None
    available_skills = session.get('available_skills', [])
    last_settings = session.get('last_settings', {})

    if request.method == 'POST':
        try:
            # 1) Считываем выбранные скилл-группы и файлы
            skill_groups = request.form.getlist('skill_groups[]')
            activity_file = request.files.get('activity')
            slots_file = request.files.get('slots')

            if not skill_groups:
                flash("Выберите хотя бы одну скилл-группу")
                return redirect(url_for('index'))
            if not activity_file:
                flash("Загрузите файл активности")
                return redirect(url_for('index'))

            # 2) Читаем стратегию и доп. параметры
            strategy = request.form.get('selection_strategy', 'by_delta')
            partial_coverage = True if request.form.get('partial_coverage') == 'on' else False

            min_interval = None
            mass_activity = None

            if strategy == 'by_delta':
                # Мин. длительность интервала
                try:
                    min_interval = int(request.form.get('min_interval', 30))
                except ValueError:
                    flash("Некорректное значение «Мин. длительность интервала»")
                    return redirect(url_for('index'))

                if not slots_file:
                    flash("Загрузите файл слотов для стратегии «Под дельту»")
                    return redirect(url_for('index'))

            else:  # strategy == 'mass'
                # В mass не нужен min_interval, но нужна выбранная активность
                mass_activity = request.form.get('mass_activity')
                if mass_activity not in ['Входящие звонки', 'Чат']:
                    flash("Выберите активность для массового назначения")
                    return redirect(url_for('index'))

            # 3) Сохраним настройки в сессии (для предзаполнения)
            session['last_settings'] = {
                'selection_strategy': strategy,
                'partial_coverage': partial_coverage,
                'min_interval': min_interval,
                'mass_activity': mass_activity
            }

            # 4) Читаем и валидируем файл активности
            try:
                df_act = load_activity(pd.read_excel(activity_file), skill_groups)
            except Exception as e:
                flash(f"Ошибка в файле активности: {e}")
                return redirect(url_for('index'))

            df_slots = None
            if strategy == 'by_delta':
                # 5) Читаем и валидируем файл слотов
                try:
                    df_slots = load_slots(pd.read_excel(slots_file))
                except Exception as e:
                    flash(f"Ошибка в файле слотов: {e}")
                    return redirect(url_for('index'))

            # 6) Запускаем логику распределения
            df_result = assign_calls(
                df_act=df_act,
                df_slots=df_slots,
                min_interval=min_interval,
                strategy=strategy,
                partial_coverage=partial_coverage,
                mass_activity=mass_activity
            )

            if df_result.empty:
                flash('Нет назначений с такими параметрами.')
            else:
                temp_id = save_temp_file(df_result)
                session['temp_id'] = temp_id
                table = df_result.to_html(
                    index=False,
                    classes='table table-bordered table-striped table-hover',
                    border=0
                )

        except Exception as e:
            logging.exception("Ошибка обработки")
            flash(f"Произошла ошибка: {e}")

    return render_template(
        'template.html',
        table=table,
        available_skills=available_skills,
        last_settings=last_settings
    )

@app.route('/extract-skills', methods=['POST'])
def extract_skills():
    # Этот эндпоинт принимает multipart: файл активности, возвращает JSON с уникальными «Скилл-группами»
    if 'activity' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    activity_file = request.files['activity']
    if activity_file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    try:
        df_activity = pd.read_excel(activity_file)
        skills = extract_unique_skills(df_activity)
        session['available_skills'] = skills
        return jsonify({'skills': skills})
    except Exception as e:
        logging.error(f"Ошибка извлечения скилл-групп: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/download')
def download():
    temp_id = session.get('temp_id')
    if not temp_id:
        flash("Нет данных для скачивания. Сначала выполните назначение.")
        return redirect(url_for('index'))
    try:
        df = load_temp_file(temp_id)
        if df.empty:
            flash("Нет данных для скачивания (пустой результат).")
            return redirect(url_for('index'))
    except FileNotFoundError:
        flash("Файл не найден. Попробуйте снова.")
        return redirect(url_for('index'))
    except Exception as e:
        flash(f"Ошибка при чтении файла: {e}")
        return redirect(url_for('index'))

    try:
        buf = generate_excel_buffer(df)
        return send_file(
            buf,
            as_attachment=True,
            download_name='assignments.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f"Ошибка при подготовке файла: {e}")
        return redirect(url_for('index'))

def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000")

if __name__ == '__main__':
    start_cleanup_thread()
    threading.Timer(1.0, open_browser).start()
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
