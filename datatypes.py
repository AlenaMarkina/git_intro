import uuid
import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class Genre:
    __slots__ = ['id', 'name', 'description', 'created', 'modified']
    id: uuid.UUID
    name: str
    description: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class FilmWork:
    __slots__ = ['id', 'title', 'description', 'creation_date', 'rating', 'type',
                 'file_path', 'created', 'modified', 'certificate']
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime.date
    rating: float
    type: str
    file_path: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class Person:
    __slots__ = ['id', 'full_name', 'created', 'modified']
    id: uuid.UUID
    full_name: str
    created: datetime.datetime
    modified: datetime.datetime


@dataclass(frozen=True)
class GenreFilmWork:
    __slots__ = ['id', 'genre_id', 'film_work_id', 'created']
    id: uuid.UUID
    genre_id: str
    film_work_id: str
    created: datetime.datetime


@dataclass(frozen=True)
class PersonFilmWork:
    __slots__ = ['id', 'person_id', 'film_work_id', 'role', 'created']
    id: uuid.UUID
    person_id: str
    film_work_id: str
    role: str
    created: datetime.datetime
