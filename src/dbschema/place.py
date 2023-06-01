from sqlalchemy.orm import Mapped, mapped_column
from dbschema import ModelBase


class Place(ModelBase):
    __tablename__ = 'place'

    id: Mapped[int] = mapped_column(
        primary_key=True
    )
    country: Mapped[str] = mapped_column(
        nullable=False
    )
    place_name: Mapped[str] = mapped_column(
        nullable=False
    )