Проект № 1.

Анализ публикуемых новостей

Общая задача: создать ETL-процесс формирования витрин данных для анализа публикаций новостей.

в папке sql_script лежат скрипты для создания таблицы БД d_news, функции end_table

в папке data - скаченные данные ("сырые")

work_psql.py - подключение к БД

parse_rss.py - парсер rss и запись в таблицу d_news

main.py - запуск задачи для Airflow - скачивание данных и запись в базу

функция end_table формирует витрину данных следующего содержания

* Суррогатный ключ категории
* Название категории
* Общее количество новостей из всех источников по данной категории за все время
* Количество новостей данной категории для каждого из источников за все время
* Общее количество новостей из всех источников по данной категории за последние сутки
* Количество новостей данной категории для каждого из источников за последние сутки
* Среднее количество публикаций по данной категории в сутки
* День, в который было сделано максимальное количество публикаций по данной категории
* Количество публикаций новостей данной категории по дням недели 
