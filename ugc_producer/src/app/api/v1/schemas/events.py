from pydantic import BaseModel, Field

from app.models.event_types import EventTypes


class Click(BaseModel):
    event_type: str = Field(
        default=EventTypes.CLICK.value,
        title="Тип события",
        description="Клик пользователя по различным ключевым элементам интерфейса",
        frozen=True,
    )
    user_id: str = Field(
        title="UUID", description="Идентификатор пользователя"
    )
    element_id: str = Field(
        title="UUID",
        description="Идентификатор элемента (фильмы, персоны, жанры и т.д.",
    )
    created_at: str = Field(title="Дата создания события")


class PageView(BaseModel):
    event_type: str = Field(
        default=EventTypes.PAGE_VIEW.value,
        title="Тип события",
        description="Данные о просмотре страницы пользователем",
        frozen=True,
    )
    user_id: str = Field(
        title="UUID", description="Идентификатор пользователя"
    )
    page_id: str = Field(title="UUID", description="Идентификатор страницы")
    element_id: str = Field(
        title="UUID",
        description="Идентификатор элемента (фильмы, персоны, жанры и т.д.",
    )
    element_info: dict[str, str] = Field(
        title="Информация об элементе",
        description="Информация о просмотренном элементе. Например о фильме:"
        "{film: Harry Potter, rating: 8, genre: fantasy, created: 2001} или"
        "{genre: comedy}",
    )
    created_at: str = Field(title="Дата создания события")


class TimeOnPage(BaseModel):
    event_type: str = Field(
        default=EventTypes.TIME_ON_PAGE.value,
        title="Тип события",
        description="Время нахождения пользователя на странице",
        frozen=True,
    )
    user_id: str = Field(
        title="UUID", description="Идентификатор пользователя"
    )
    element_id: str = Field(
        title="UUID",
        description="Идентификатор элемента (фильмы, персоны, жанры и т.д.",
    )
    element_info: dict[str, str] = Field(
        title="Информация об элементе",
        description="Информация о просмотренном элементе. Например о фильме:"
        "{'film': 'Harry Potter', 'rating': 8, 'genre': 'fantasy', 'created': 2001} или"
        "{'genre': 'comedy'}",
    )
    duration: int = Field(
        title="Продолжительность",
        description="Продолжительность нахождения пользователя на странице, в секундах.",
    )
    created_at: str = Field(title="Дата создания события")


class ChangeVideoQuality(BaseModel):
    event_type: str = Field(
        default=EventTypes.CHANGE_VIDEO_QUALITY.value,
        title="Тип события",
        description="Смена качества видео пользователем",
        frozen=True,
    )
    user_id: str = Field(
        title="UUID", description="Идентификатор пользователя"
    )
    video_id: str = Field(title="UUID", description="Идентификатор видео")
    quality_before: int = Field(
        title="Качество видео",
        description="Качество видео до изменения пользователем",
    )
    quality_after: int = Field(
        title="Качество видео",
        description="Качество видео после изменения пользователем",
    )
    created_at: str = Field(title="Дата создания события")


class WatchToTheEnd(BaseModel):
    event_type: str = Field(
        default=EventTypes.WATCH_TO_THE_END.value,
        title="Тип события",
        description="Просмотр видео пользователем до конца",
        frozen=True,
    )
    user_id: str = Field(
        title="UUID", description="Идентификатор пользователя"
    )
    video_id: str = Field(title="UUID", description="Идентификатор видео")
    duration: int = Field(
        title="Продолжительность видео",
        description="Продолжительность видео, в секундах.",
    )
    viewed: int = Field(
        title="Продолжительность видео",
        description="Просмотрено пользователем, в секундах.",
    )
    created_at: str = Field(title="Дата создания события")


class UsingSearchFilters(BaseModel):
    event_type: str = Field(
        default=EventTypes.USING_SEARCH_FILTERS.value,
        title="Тип события",
        description="Использование фильтров поиска пользователем",
        frozen=True,
    )
    user_id: str = Field(
        title="UUID", description="Идентификатор пользователя"
    )
    filters: dict[str, str] = Field(
        title="Фильтры",
        description="Фильтры, используемые пользователем для поиска (например, по жанрам, дате выхода, рейтингу)"
        "{'rating': '8+'}",
    )
    created_at: str = Field(title="Дата создания события")
