import os
import logging
import sqlite3
from sqlite3 import Connection

from datatypes import Genre, FilmWork, Person, GenreFilmWork, PersonFilmWork


class SQLiteLoader:
    def __init__(self, sqlite_conn: Connection):
        self._conn = sqlite_conn
        self._conn.row_factory = sqlite3.Row
        self._cursor = self.conn.cursor()
        self._logger = logging.getLogger('sqlite')
        self._db = os.environ.get('DB_SQLITE')

    @property
    def conn(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    @property
    def sqlite_logger(self):
        return self._logger

    def load_genres(self, table_name: str) -> list[Genre]:
        return self.worker(table_name, Genre)

    def load_movies(self, table_name: str) -> list[FilmWork]:
        return self.worker(table_name, FilmWork)

    def load_persons(self, table_name: str) -> list[Person]:
        return self.worker(table_name, Person)

    def load_genre_film_work(self, table_name: str) -> list[GenreFilmWork]:
        return self.worker(table_name, GenreFilmWork)

    def load_person_film_work(self, table_name: str) -> list[PersonFilmWork]:
        return self.worker(table_name, PersonFilmWork)

    @staticmethod
    def change_filed_name(data: dict) -> dict:
        if 'created_at' in data.keys():
            data['created'] = data.pop('created_at')
        if 'updated_at' in data.keys():
            data['modified'] = data.pop('updated_at')
        return data

    def worker(self, table_name: str, cls) -> list:
        """Загрузка данных из SQLite"""
        list_of_class = []
        try:
            self.sqlite_logger.info(f"Загрузка данных из БД '{self._db}', табл. '{table_name}'..")

            self.cursor.execute(f'SELECT * FROM {table_name}')
            rows = self.cursor.fetchall()

            if not rows:
                self.sqlite_logger.warning(f"Таблица '{table_name}' пустая")
                return rows  # []

            data_dict = [dict(row) for row in rows]
            data_list = list(map(self.change_filed_name, data_dict))
            list_of_class = [cls(**dict_) for dict_ in data_list]
        except sqlite3.OperationalError:
            self.sqlite_logger.exception(f'Ошибка при чтении из sqlite:')
        else:
            self.sqlite_logger.info(f"Загрузка завершена успешно.")
        return list_of_class

