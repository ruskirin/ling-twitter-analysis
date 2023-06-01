from typing import List, Optional
from sqlalchemy import String, Integer, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from dbschema import ModelBase
from datetime import datetime


class User(ModelBase):
    __tablename__ = 'user'

    id: Mapped[int] = mapped_column(
        primary_key=True
    )
    username: Mapped[str] = mapped_column(
        nullable=False
    )
    name: Mapped[str] = mapped_column(
        nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False
    )
    place_name: Mapped[str] = mapped_column(
        nullable=True
    )
    followers_count: Mapped[int] = mapped_column(
        nullable=False
    )
    following_count: Mapped[int] = mapped_column(
        nullable=False
    )
    tweet_count: Mapped[int] = mapped_column(
        nullable=False
    )
    listed_count: Mapped[int] = mapped_column(
        nullable=False
    )

