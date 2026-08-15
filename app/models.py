import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, Text, DateTime, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class DeadLetterQueue(Base):
    __tablename__ = "dead_letter_queue"
    
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        nullable=False
    )
    payload = Column(
        JSONB,
        nullable=False
    )
    replay_count = Column(
        Integer,
        server_default=text("0"),
        nullable=False
    )
    stack_trace = Column(
        Text,
        nullable=True
    )
    created_at = Column(
        DateTime(timezone=True),
        server_default=text("now()"),
        nullable=False
    )
