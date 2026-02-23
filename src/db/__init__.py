# src/db/__init__.py
"""
Database management module
"""
from .duckdb_manager import DuckDBManager, get_duckdb_manager

__all__ = ['DuckDBManager', 'get_duckdb_manager']
