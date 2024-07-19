import os
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.environ.get("RUDY_ENV_FILE", ".env"),
        env_file_encoding="utf-8",
    )

    # logging
    LOG_LEVEL: Optional[str] = Field(
        description="The level of the logs", default="INFO"
    )
    LOG_STDOUT: Optional[bool] = Field(
        description="Whether to log to stdout", default=False
    )
    LOG_FORMAT: Optional[str] = Field(
        description="The format of the logs",
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


settings = Settings()
