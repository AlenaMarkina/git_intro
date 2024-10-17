from pydantic import BaseModel, Field


class KafkaPayload(BaseModel):
    topic: str | None = Field(
        title="Топик",
        description="Логическая категория, в пределах которой продюсеры отправляю сообщения, а потребители читают их",
    )
    key: str = Field(title="Ключ", description="Ключ события")
    value: str = Field(title="Значение события")
