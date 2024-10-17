from app.settings.base import Settings


class JwtSettings(Settings):
    AUTH_API_URL: str = "http://127.0.0.1:8080/api/v1"
    JWT_ALGORITHM: str = "HS256"
    AUDIENCE: str = "fastapi"
    SECRET: str = "c1b24fa003dfcc39761196e00113a0fc47d0f07a"


settings = JwtSettings()
