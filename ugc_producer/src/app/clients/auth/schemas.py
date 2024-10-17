from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr


class UserRetrieveSchema(BaseModel):
    id: UUID
    email: EmailStr
    is_active: bool = True
    is_superuser: bool = False
    is_verified: bool = False

    model_config = ConfigDict(from_attributes=True)
