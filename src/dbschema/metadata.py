from sqlalchemy.orm import Mapped, mapped_column
from dbschema import ModelBase
from utils import types


class Metadata(ModelBase):
    __tablename__ = 'metadata'

    id: Mapped[str] = mapped_column(
        primary_key=True
    )
    value: Mapped[str] = mapped_column(
        nullable=False
    )
    parameter: Mapped[str] = mapped_column(
        nullable=False
    )
    detail: Mapped[str] = mapped_column(
        nullable=False
    )
    title: Mapped[str] = mapped_column(
        nullable=True
    )
    resource_type: Mapped[types.MetaType] = mapped_column(
        nullable=False
    )
    type: Mapped[str] = mapped_column(
        nullable=False
    )