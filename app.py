from flask import Flask, request, render_template, send_file, redirect, url_for, flash, session, jsonify
from processing import load_activity, load_slots, assign_calls, extract_unique_skills
from utils import save_temp_file, load_temp_file, generate_excel_buffer
from cleanup import start_cleanup_thread
from config import SECRET_KEY, TEMP_DIR
import webbrowser
import threading
import logging
import pandas as pd
import sys
import os

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Инициализация Flask ---
if getattr(sys, 'frozen', False):
    import sys
    if hasattr(sys, '_MEIPASS'):
        template_folder = os.path.join(sys._MEIPASS, 'templates')
        app = Flask(__name__, template_folder=template_folder)
    else:
        app = Flask(__name__)
else:
    app = Flask(__name__)

app.secret_key = SECRET_KEY
TEMPLATE_NAME = 'template.html'

# --- Перед каждым запросом ---
@app.before_request
def before_request():
    session.permanent = False
    logging.info("Начало запроса")

# --- Главная страница ---
@app.route('/', methods=['GET', 'POST'])
def index():
    table_html = None
    available_skills = session.get('available_skills', [])
    last_settings = session.get('last_settings', {})

    if request.method == 'POST':
        try:
            # Получаем данные из формы
            skill_groups = request.form.getlist('skill_groups[]')
            activity_file = request.files.get('activity')
            slots_file = request.files.get('slots')
            selection_strategy = request.form.get('selection_strategy', 'by_delta')
            if selection_strategy not in ['by_delta', 'mass']:
                selection_strategy = 'by_delta'
            partial_coverage = 'partial_coverage' in request.form
            mass_activity = request.form.get('mass_activity', 'Входящие звонки')
            min_interval = int(request.form.get('min_interval', 30))

            # Проверяем обязательные поля
            if not skill_groups:
                flash('Выберите хотя бы одну скилл-группу')
                return render_template(TEMPLATE_NAME,
                                       table=table_html,
                                       available_skills=available_skills,
                                       last_settings=last_settings)

            if not activity_file or activity_file.filename == '':
                flash('Загрузите файл активности')
                return render_template(TEMPLATE_NAME,
                                       table=table_html,
                                       available_skills=available_skills,
                                       last_settings=last_settings)

            if selection_strategy == 'by_delta' and (not slots_file or slots_file.filename == ''):
                flash('Загрузите файл слотов')
                return render_template(TEMPLATE_NAME,
                                       table=table_html,
                                       available_skills=available_skills,
                                       last_settings=last_settings)

            # Сохраняем настройки
            session['last_settings'] = {
                'skill_groups': skill_groups,
                'selection_strategy': selection_strategy,
                'partial_coverage': partial_coverage,
                'mass_activity': mass_activity,
                'min_interval': min_interval
            }
            session.modified = True

            # Обрабатываем файл активности
            activity_df = load_activity(activity_file, skill_groups)

            # Обрабатываем файл слотов (если нужен)
            slots_df = None
            if selection_strategy == 'by_delta':
                slots_df = load_slots(slots_file)

            # Назначаем активность
            result_df = assign_calls(
                df_act=activity_df,
                df_slots=slots_df,
                min_interval=min_interval,
                strategy=selection_strategy,
                partial_coverage=partial_coverage,
                mass_activity=mass_activity
            )

            # Сохраняем результат
            if result_df.empty:
                raise ValueError("Нет данных для сохранения — результат пуст")

            temp_id = save_temp_file(result_df)
            session['temp_id'] = temp_id
            session.modified = True

            # Генерируем предпросмотр
            table_html = result_df.to_html(classes='table table-striped table-bordered', index=False)

            # Перерисовываем страницу с результатом
            return render_template(TEMPLATE_NAME,
                                   table=table_html,
                                   available_skills=available_skills,
                                   last_settings=session.get('last_settings', {}))

        except Exception as e:
            logging.exception("Ошибка обработки запроса")
            flash(f"Произошла ошибка: {e}")
            return render_template(TEMPLATE_NAME,
                                   table=table_html,
                                   available_skills=available_skills,
                                   last_settings=last_settings)

    return render_template(TEMPLATE_NAME,
                           table=table_html,
                           available_skills=available_skills,
                           last_settings=session.get('last_settings', {}))

# --- Извлечение скилл-групп из файла ---
@app.route('/extract-skills', methods=['POST'])
def extract_skills():
    if 'activity' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['activity']
    if file.filename == '':
        return jsonify({'error': 'Empty filename'}), 400

    try:
        df = pd.read_excel(file)
        skills = extract_unique_skills(df)
        session['available_skills'] = skills
        session.modified = True
        return jsonify({'skills': skills})
    except Exception as e:
        logging.exception("Ошибка извлечения скилл-групп")
        return jsonify({'error': str(e)}), 500

# --- Скачивание результата ---
@app.route('/download')
def download():
    temp_id = session.get('temp_id')
    if not temp_id:
        flash('Нет данных для скачивания. Сначала выполните назначение.')
        return redirect(url_for('index'))

    try:
        df = load_temp_file(temp_id)
        if df.empty:
            raise ValueError("Невозможно скачать файл — данные пусты")

        buffer = generate_excel_buffer(df)
        return send_file(
            buffer,
            as_attachment=True,
            download_name='result.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logging.exception("Ошибка при скачивании")
        flash(f"Ошибка при скачивании файла: {e}")
        return redirect(url_for('index'))

# --- Открытие браузера ---
def open_browser():
    webbrowser.open_new('http://127.0.0.1:5000')

# --- Запуск приложения ---
if __name__ == '__main__':
    start_cleanup_thread()
    threading.Timer(1.0, open_browser).start()
    app.run(debug=False, host='127.0.0.1', port=5000)