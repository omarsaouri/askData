from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from db import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    row_count = Column(Integer, nullable=False)
    column_count = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    columns = relationship("ColumnProfile", back_populates="dataset", cascade="all, delete-orphan")
    relationships = relationship("Relationship", back_populates="dataset", cascade="all, delete-orphan")
    insights = relationship("Insight", back_populates="dataset", cascade="all, delete-orphan")


class ColumnProfile(Base):
    __tablename__ = "column_profiles"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    inferred_type = Column(String, nullable=False)
    semantic_type = Column(String, nullable=False)
    stats = Column(JSONB, nullable=False)
    sample_values = Column(JSONB, nullable=False)

    dataset = relationship("Dataset", back_populates="columns")


class Relationship(Base):
    __tablename__ = "relationships"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    col_a = Column(String, nullable=False)
    col_b = Column(String, nullable=False)
    relation_type = Column(String, nullable=False)
    strength = Column(Float, nullable=True)
    details = Column(JSONB, nullable=True)

    dataset = relationship("Dataset", back_populates="relationships")


class Insight(Base):
    __tablename__ = "insights"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    title = Column(String, nullable=False)
    detail = Column(Text, nullable=False)
    severity = Column(String, nullable=False, default="info")

    dataset = relationship("Dataset", back_populates="insights")
