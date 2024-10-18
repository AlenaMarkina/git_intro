from app.settings.base import Settings


class ApiSettings(Settings):
    TITLE: str = "auth-api"
    OPENAPI_URL: str = "/api/auth/openapi.json"
    DOCS_URL: str = "/api/auth/docs"
    REDOC_URL: str = "/api/auth/redoc"
    SECRET_KEY: str


settings = ApiSettings()
