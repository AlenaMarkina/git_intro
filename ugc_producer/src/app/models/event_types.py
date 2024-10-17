import enum


class Topics(enum.Enum):
    CLICKS = "clicks"
    PAGE_VIEWS = "page_views"
    CUSTOM_EVENTS = "custom_events"


class EventTypes(enum.Enum):
    CLICK = "click"
    PAGE_VIEW = "page_view"
    TIME_ON_PAGE = "time_on_page"
    CHANGE_VIDEO_QUALITY = "change_video_quality"
    WATCH_TO_THE_END = "watch_to_the_end"
    USING_SEARCH_FILTERS = "using_search_filters"


class Groups(enum.Enum):
    clicks = [EventTypes.CLICK.value]
    page_views = [EventTypes.PAGE_VIEW.value, EventTypes.TIME_ON_PAGE.value]
    custom_events = [
        EventTypes.CHANGE_VIDEO_QUALITY.value,
        EventTypes.WATCH_TO_THE_END.value,
        EventTypes.USING_SEARCH_FILTERS.value,
    ]


async def get_topic_by_event(event: str) -> str | None:
    try:
        [topic] = [i.name for i in Groups if event in i.value]
    except ValueError:
        return
    return topic
