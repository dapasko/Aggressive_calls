FULL_TEMPLATE = '''
<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Назначение активности</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Roboto&display=swap" rel="stylesheet">
    <link href="https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/css/select2.min.css" rel="stylesheet">
    <style>
        body { font-family: 'Roboto', sans-serif; background-color: #ffffff; color: #000000; }
        .navbar { background-color: #ffffff; border-bottom: 1px solid #e0e0e0; }
        .navbar-brand { font-weight: bold; color: #000000; }
        .btn-primary { background-color: #FFD700; border-color: #FFD700; color: #000000; }
        .btn-primary:hover { background-color: #FFC300; border-color: #FFC300; color: #000000; }
        #loading-overlay { 
            position: fixed; top: 0; left: 0; width: 100%; height: 100%; 
            background: rgba(255, 255, 255, 0.8); display: flex; 
            align-items: center; justify-content: center; z-index: 2000; display: none; 
        }
        .spinner-border { width: 3rem; height: 3rem; color: #FFD700; }
        .select2-container .select2-selection--multiple { min-height: 38px; }
    </style>
</head>
<body>
    <nav class="navbar navbar-light">
        <div class="container">
            <a class="navbar-brand" href="#">
                <img src="https://www.tbank.ru/favicon.ico" alt="Т-Банк" width="30" height="30" class="d-inline-block align-text-top">
                Т-Банк
            </a>
        </div>
    </nav>

    <div id="loading-overlay">
        <div class="text-center">
            <div class="spinner-border" role="status">
                <span class="visually-hidden">Загрузка...</span>
            </div>
            <div class="mt-3">Обработка данных, пожалуйста, ждите...</div>
        </div>
    </div>

    <div class="container py-5">
        <h1 class="mb-4">📞 Автоматическое назначение активности</h1>

        {% with messages = get_flashed_messages() %}
            {% if messages %}
                {% for msg in messages %}
                    <div class="alert alert-warning">{{ msg }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}

        <form id="upload-form" method="post" enctype="multipart/form-data">
            <div class="mb-3">
                <label class="form-label">Скилл-группы (выберите одну или несколько)</label>
                <select class="form-select" name="skill_groups" id="skill-groups-select" multiple="multiple" required>
                    {% if available_skills %}
                        {% for skill in available_skills %}
                            <option value="{{ skill }}">{{ skill }}</option>
                        {% endfor %}
                    {% endif %}
                </select>
                <div class="form-text">Удерживайте Ctrl (Cmd на Mac) для выбора нескольких групп</div>
            </div>
            <div class="mb-3">
                <label class="form-label">Активность (Omni-чат + Входящие звонки)</label>
                <input class="form-control" type="file" name="activity" id="activity-file" accept=".xlsx" required>
            </div>
            <div class="mb-3">
                <label class="form-label">Слоты с Дельтой</label>
                <input class="form-control" type="file" name="slots" accept=".xlsx" required>
            </div>
            <button id="submit-btn" class="btn btn-primary" type="submit">Запустить</button>
        </form>

        {% if table %}
            <hr>
            <h2 class="mt-4">Назначения</h2>
            <div class="table-responsive">
                {{ table|safe }}
            </div>
            <a href="{{ url_for('download') }}" class="btn btn-success mt-3">Скачать Excel</a>
        {% endif %}
    </div>

    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/select2@4.1.0-rc.0/dist/js/select2.min.js"></script>
    <script>
        $(document).ready(function() {
            // Инициализация Select2
            $('#skill-groups-select').select2({
                placeholder: "Выберите скилл-группы",
                allowClear: true,
                width: '100%'
            });

            // Обновление списка скилл-групп при загрузке файла активности
            $('#activity-file').change(function() {
                if (this.files.length > 0) {
                    const formData = new FormData();
                    formData.append('activity', this.files[0]);

                    $('#loading-overlay').show();

                    fetch('/extract-skills', {
                        method: 'POST',
                        body: formData
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (data.skills && data.skills.length > 0) {
                            $('#skill-groups-select').empty();
                            data.skills.forEach(skill => {
                                $('#skill-groups-select').append(new Option(skill, skill));
                            });
                            $('#skill-groups-select').trigger('change');
                        }
                    })
                    .catch(error => {
                        console.error('Error:', error);
                    })
                    .finally(() => {
                        $('#loading-overlay').hide();
                    });
                }
            });

            // Отправка формы
            $('#upload-form').on('submit', function(e) {
                $('#loading-overlay').show();
                $('#submit-btn').prop('disabled', true);
            });
        });
    </script>
</body>
</html>
'''