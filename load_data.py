import os
import logging
import sqlite3
from dotenv import load_dotenv

import config_logging
from sqlite_loader import SQLiteLoader
from postgres_saver import PostgresSaver

import psycopg2
from psycopg import connection as _connection


GENRE = 'genre'
FILM_WORK = 'film_work'
PERSON = 'person'
GENRE_FILM_WORK = 'genre_film_work'
PERSON_FILM_WORK = 'person_film_work'


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    sqlite_loader = SQLiteLoader(sqlite_conn)
    postgres_saver = PostgresSaver(pg_conn)

    data = sqlite_loader.load_genres(GENRE)
    postgres_saver.save_all_data(GENRE, data)

    data = sqlite_loader.load_movies(FILM_WORK)
    postgres_saver.save_all_data(FILM_WORK, data)

    data = sqlite_loader.load_persons(PERSON)
    postgres_saver.save_all_data(PERSON, data)

    data = sqlite_loader.load_genre_film_work(GENRE_FILM_WORK)
    postgres_saver.save_all_data(GENRE_FILM_WORK, data)

    data = sqlite_loader.load_person_film_work(PERSON_FILM_WORK)
    postgres_saver.save_all_data(PERSON_FILM_WORK, data)


if __name__ == '__main__':
    load_dotenv()
    config_logging.init_logging()

    logger = logging.getLogger('main')

    logger.info("Запуск сервиса 'sqlite_to_postgres'!")
    logger.info('Инициализация логирования')

    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('HOST'),
        'port': os.environ.get('PORT')
    }

    sqlite_db = os.environ.get('DB_SQLITE')

    if not os.path.isfile(sqlite_db):
        logger.error(f'Базы данных {sqlite_db} не существует.')
    else:
        try:
            with (sqlite3.connect(sqlite_db) as sqlite_conn,
                  psycopg2.connect(**dsl) as pg_conn):
                load_from_sqlite(sqlite_conn, pg_conn)
        except psycopg2.OperationalError:
            logger.exception(f"Ошибка подключения к БД '{os.environ.get('DB_NAME')}'")
