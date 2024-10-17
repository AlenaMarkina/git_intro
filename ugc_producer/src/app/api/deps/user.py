from typing import Annotated

import jwt
from fastapi import Depends, status
from fastapi.exceptions import HTTPException
from fastapi.security import OAuth2PasswordBearer

from app.clients.auth.client import auth_client
from app.clients.auth.schemas import UserRetrieveSchema
from app.settings.jwt import settings

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def decode_jwt(token: str) -> dict | None:
    try:
        decoded_token = jwt.decode(
            token,
            settings.SECRET,
            settings.AUDIENCE,
            settings.JWT_ALGORITHM,
        )
    except jwt.PyJWTError:
        return None
    except ValueError:
        return None
    return decoded_token


async def check_user(
    token: str = Depends(oauth2_scheme),
) -> UserRetrieveSchema:
    try:
        return await auth_client.check(token)
    except Exception:
        data = decode_jwt(token)

        if not data:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

        return UserRetrieveSchema(**data)


UserData = Annotated[UserRetrieveSchema, Depends(check_user)]
