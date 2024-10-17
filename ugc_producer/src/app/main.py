import json
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from app.api.deps import kafka
from app.api.v1.router import router
from app.settings.api import settings as api_settings
from app.settings.kafka import settings as kafka_settings


def serializer(value):
    return json.dumps(value).encode()


@asynccontextmanager
async def lifespan(_: FastAPI):
    kafka.kafka_producer = AIOKafkaProducer(
        client_id="ugc_producer",
        bootstrap_servers=f"{kafka_settings.KAFKA_HOST}:{kafka_settings.KAFKA_PORT}",
        value_serializer=serializer,
        key_serializer=serializer,
        compression_type="gzip",
    )
    await kafka.kafka_producer.start()
    yield
    await kafka.kafka_producer.stop()


app = FastAPI(
    title=api_settings.TITLE,
    openapi_url=api_settings.OPENAPI_URL,
    docs_url=api_settings.DOCS_URL,
    redoc_url=api_settings.REDOC_URL,
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.include_router(router)
