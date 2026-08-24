"""
Shared PostgreSQL (warehouse) connection helpers.

One place holds the connection settings, used both by the Spark JDBC loader
and by the dashboard, which reads its figures back with plain SQL.
"""
import os


def pg_settings():
    return {
        "host": os.environ.get("POSTGRES_HOST", "postgres"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "database": os.environ.get("POSTGRES_DB", "warehouse"),
        "user": os.environ.get("POSTGRES_USER", "warehouse"),
        "password": os.environ.get("POSTGRES_PASSWORD", "warehouse"),
    }


def jdbc_url():
    s = pg_settings()
    return f"jdbc:postgresql://{s['host']}:{s['port']}/{s['database']}"


def jdbc_properties():
    s = pg_settings()
    return {
        "user": s["user"],
        "password": s["password"],
        "driver": "org.postgresql.Driver",
    }


def connect():
    """psycopg2 connection, for the steps that do not need Spark."""
    import psycopg2

    return psycopg2.connect(**pg_settings())
