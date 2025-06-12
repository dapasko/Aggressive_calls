# 📌 TimeFlow: Автоматическое назначение активности

> 🎯 **Автоматизированное распределение активности сотрудникам на основе данных из Excel**

[![Python](https://img.shields.io/badge/Python-3.9+-blue)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/Flask-2.3+-green)](https://flask.palletsprojects.com/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-orange)](https://pandas.pydata.org/)

---

## 🧠 Описание

**TimeFlow** — это веб-приложение, которое автоматически назначает активность сотрудникам на основе:
- **Файла активности** (Excel)
- **Слоты с дельтой** (Excel)

Приложение поддерживает **две стратегии назначения**:
1. **Под дельту** — назначение по слотам с дельтой (например, +2 ч на чат, -1 ч на звонки)
2. **Массовое** — массовое назначение по расписанию

---

## 📦 Технологии

- 🐍 **Python 3.9+**
- 🌐 **Flask** — веб-фреймворк
- 📊 **Pandas** — обработка данных
- 📁 **OpenPyXL** — работа с Excel
- 🖼️ **Bootstrap 5** — интерфейс
- 🔍 **Select2** — мультиселект
- 📋 **DataTables** — отображение таблицы
- 📁 **Werkzeug** — загрузка файлов

---

## 📁 Структура проекта
Aggressive_calls/
│
├── app.py # Основное Flask-приложение
├── config.py # Конфигурация
├── processing.py # Логика обработки данных
├── utils.py # Вспомогательные функции
├── cleanup.py # Очистка временных файлов
├── templates/
│ └── template.html # HTML-интерфейс
├── README.md # Это руководство
└── requirements.txt # Зависимости

---

## 🛠️ Установка

## 1. Клонируйте репозиторий:
```bash
git clone https://github.com/dapasko/Aggressive_calls.git
cd Aggressive_calls
```
## 2. Установите зависимости:
```bash
install -r requirements.txt
```

## 3.📝 Как использовать
```bash
1. Загрузите файл активности (Excel)
2. Выберите скилл-группы
3. Выберите стратегию назначения:
•  by_delta  — назначение по слотам с дельтой
•  mass  — массовое назначение по расписанию
4. Нажмите "Запустить"
5. Скачайте результаты в формате Excel
```
