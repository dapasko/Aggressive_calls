from flask import Flask, request, render_template_string, send_file, redirect, url_for, flash, session, jsonify
from processing import load_activity, load_slots, assign_calls, extract_unique_skills
from utils import save_temp_file, load_temp_file, generate_excel_buffer
from cleanup import start_cleanup_thread
from templates import FULL_TEMPLATE
from config import SECRET_KEY
import webbrowser
import threading
import pandas as pd
import logging
import io

app = Flask(__name__)
app.secret_key = SECRET_KEY


@app.before_request
def before_request():
    session.permanent = False


@app.route('/', methods=['GET', 'POST'])
def index():
    table = None
    available_skills = session.get('available_skills', [])

    if request.method == 'POST':
        try:
            skill_groups = request.form.getlist('skill_groups')
            activity_file = request.files.get('activity')
            slots_file = request.files.get('slots')

            if not skill_groups:
                flash("Выберите хотя бы одну скилл-группу")
                return redirect(url_for('index'))

            if not activity_file or not slots_file:
                flash("Не все файлы загружены")
                return redirect(url_for('index'))

            try:
                df_act = load_activity(pd.read_excel(activity_file), skill_groups)
            except Exception as e:
                flash(f"Ошибка в файле активности: {e}")
                return redirect(url_for('index'))

            try:
                df_slots = load_slots(pd.read_excel(slots_file))
            except Exception as e:
                flash(f"Ошибка в файле слотов: {e}")
                return redirect(url_for('index'))

            df_result = assign_calls(df_act, df_slots)

            if df_result.empty:
                flash('Нет назначений.')
            else:
                temp_id = save_temp_file(df_result)
                session['temp_id'] = temp_id
                table = df_result.to_html(
                    index=False,
                    classes='table table-bordered table-striped',
                    border=0
                )
        except Exception as e:
            logging.exception("Ошибка обработки")
            flash(f"Произошла ошибка: {e}")

    return render_template_string(FULL_TEMPLATE, table=table, available_skills=available_skills)


@app.route('/extract-skills', methods=['POST'])
def extract_skills():
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
        flash("Нет данных для скачивания. Сначала выполните обработку.")
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