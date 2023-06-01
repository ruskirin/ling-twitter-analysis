from ast import literal_eval
from datetime import datetime
from sqlalchemy.dialects.postgresql import ARRAY
from typing import Optional

import numpy as np
from sqlalchemy import String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, validates

from dbschema import ModelBase
from utils import types


class Tweet(ModelBase):
    __tablename__ = 'tweet'

    # datetime when record was extracted
    extract_time: Mapped[datetime] = mapped_column(
        DateTime, nullable=False
    )
    # verb of interest used for extraction
    extract_verb: Mapped[types.Verb] = mapped_column(
        nullable=False
    )
    id: Mapped[int] = mapped_column(
        primary_key=True
    )
    text_orig: Mapped[str] = mapped_column(
        String, nullable=False
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey('user.id'), nullable=False
    )
    """---------------------------------"""
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False
    )

    @validates('created_at')
    def validate_created_at(self, key, time):
        return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ')

    """---------------------------------"""

    lang: Mapped[types.Lang] = mapped_column(
        nullable=False
    )
    tweet_place_id: Mapped[str] = mapped_column(
        ForeignKey('place.id'), nullable=False
    )
    geo_coord_type: Mapped[Optional[str]] = mapped_column(
        nullable=True
    )
    geo_coord: Mapped[Optional[str]] = mapped_column(
        nullable=True
    )

    """Relationships"""
    """---------------------------------"""
    replied_tweets = mapped_column(ARRAY(String), nullable=True)

    @validates('replied_tweets')
    def validate_replied_tweets(self, key, tweets):
        """Extract the replied tweets from 'referenced_tweets' column"""
        if tweets is np.nan:
            return None

        replies = {t['id'] for t in literal_eval(tweets)}
        return replies

    """---------------------------------"""
    mentions = mapped_column(ARRAY(String), nullable=True)

    @validates('mentions')
    def validate_mentions(self, key, mentions):
        """Extract the mentioned tweet ids"""
        if mentions is np.nan:
            return None

        users = {m['id'] for m in literal_eval(mentions)}
        return users

    """---------------------------------"""
    edit_history_tweet_ids = mapped_column(ARRAY(String), nullable=False)

    @validates('edit_history_tweet_ids')
    def validate_edit_history_tweet_ids(self, key, ids):
        """Extract the previous tweet ids of edited tweets"""
        edits = {i for i in literal_eval(ids)}
        return edits

    """---------------------------------"""

    """Response metrics"""
    retweet_count: Mapped[int] = mapped_column(
        nullable=False
    )
    reply_count: Mapped[int] = mapped_column(
        nullable=False
    )
    like_count: Mapped[int] = mapped_column(
        nullable=False
    )
    quote_count: Mapped[int] = mapped_column(
        nullable=False
    )
    impression_count: Mapped[int] = mapped_column(
        nullable=False
    )
