import logging
import os
from dataclasses import astuple, fields

import psycopg2
from psycopg2.extras import execute_values

from datatypes import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


class PostgresSaver:
    def __init__(self, pg_conn):
        self._conn = pg_conn
        self._cursor = self.conn.cursor()
        self._logger = logging.getLogger("postgres")
        self._db = os.environ.get("DB_NAME")
        self._schema = os.environ.get("SCHEMA")

    @property
    def conn(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    @property
    def pg_logger(self):
        return self._logger

    @staticmethod
    def str_to_class(table_name: str):
        switcher = {
            "genre": Genre,
            "film_work": FilmWork,
            "person": Person,
            "genre_film_work": GenreFilmWork,
            "person_film_work": PersonFilmWork,
        }
        return switcher.get(table_name)

    def save_all_data(self, table_name: str, data: list) -> None:
        """Загрузка данных в Postgres"""
        column_names_str = ""
        data_to_pass = []

        self.pg_logger.info(
            f"Загрузка данных в БД '{self._db}', табл.'{self._schema}.{table_name}'.."
        )

        try:
            column_names = [
                field.name for field in fields(self.str_to_class(table_name))
            ]
            column_names_str = ", ".join(column_names)
            data_to_pass = [astuple(row) for row in data]
        except TypeError as e:
            self.pg_logger.exception(f"TypeError - {e}")

        try:
            query = (
                f"INSERT INTO content.{table_name} ({column_names_str}) VALUES %s"
                f"ON CONFLICT (id) DO NOTHING"
            )
            execute_values(self.cursor, query, data_to_pass)
        except psycopg2.errors.UndefinedTable:
            self.pg_logger.exception("UndefinedTable:")
        except psycopg2.errors.UndefinedColumn:
            self.pg_logger.exception("UndefinedColumn:")
        except psycopg2.errors.SyntaxError:
            self.pg_logger.exception("SyntaxError:")

        self.pg_logger.info("Загрузка данных завершена успешно.")
