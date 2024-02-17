"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2
import os


def writing_to_db_from_csv_file(filepath: str, table_name: str) -> None:
    """
    Функция записи таблиц в БД из csv файла.
    :param filepath: Путь к файлу.
    :param table_name: Название Таблицы.
    :return: None.
    """

    connection = psycopg2.connect(
        host='localhost',
        database='north',
        user='postgres',
        password='12345SS'
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                with open(os.path.join(filepath), encoding='utf8') as file:
                    rows = csv.reader(file)
                    next(rows)
                    for row in rows:
                        count_placeholder = '%s, ' * len(row)
                        reform_placeholder = count_placeholder.rstrip(', ')
                        cursor.execute(f"INSERT INTO {table_name} VALUES ({reform_placeholder})", row)

    finally:
        connection.close()


writing_to_db_from_csv_file('north_data/employees_data.csv',
                            "employees")
writing_to_db_from_csv_file('north_data/customers_data.csv',
                            "customers")
writing_to_db_from_csv_file('north_data/orders_data.csv',
                            "orders")
