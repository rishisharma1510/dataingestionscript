#!/usr/bin/env python3
"""
Iceberg to SQL Server Data Ingestion Script

Reads data from Apache Iceberg tables and ingests it into Microsoft SQL Server.
Configuration is provided via a YAML config file or command-line arguments.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pymssql
import yaml
from pyiceberg.catalog import load_catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Mapping from Iceberg/PyArrow types to SQL Server types
ICEBERG_TO_SQL_TYPE_MAP = {
    "bool": "BIT",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "SMALLINT",
    "uint16": "INT",
    "uint32": "BIGINT",
    "uint64": "BIGINT",
    "float16": "REAL",
    "float32": "REAL",
    "float": "REAL",
    "float64": "FLOAT",
    "double": "FLOAT",
    "decimal128": "DECIMAL",
    "date32": "DATE",
    "date64": "DATE",
    "timestamp": "DATETIME2",
    "time32": "TIME",
    "time64": "TIME",
    "string": "NVARCHAR(MAX)",
    "large_string": "NVARCHAR(MAX)",
    "utf8": "NVARCHAR(MAX)",
    "large_utf8": "NVARCHAR(MAX)",
    "binary": "VARBINARY(MAX)",
    "large_binary": "VARBINARY(MAX)",
    "fixed_size_binary": "VARBINARY(MAX)",
}


def load_config(config_path: str) -> dict:
    """Load configuration from a YAML file."""
    path = Path(config_path)
    if not path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)
    with open(path) as f:
        config = yaml.safe_load(f)
    logger.info("Loaded config from %s", config_path)
    return config


def get_iceberg_table(config: dict):
    """Connect to the Iceberg catalog and load the specified table."""
    catalog_cfg = config["iceberg"]
    catalog_props = catalog_cfg.get("catalog_properties", {})
    catalog_props["name"] = catalog_cfg.get("catalog_name", "default")

    catalog = load_catalog(**catalog_props)

    namespace = catalog_cfg["namespace"]
    table_name = catalog_cfg["table"]
    full_table = f"{namespace}.{table_name}"

    logger.info("Loading Iceberg table: %s", full_table)
    table = catalog.load_table(full_table)
    logger.info("Iceberg table loaded. Schema: %s", table.schema())
    return table


def arrow_type_to_sql(arrow_type) -> str:
    """Convert a PyArrow type to SQL Server column type."""
    type_str = str(arrow_type).lower()

    # Handle parameterized types
    if type_str.startswith("decimal"):
        # Extract precision and scale from e.g. decimal128(10, 2)
        import re
        match = re.search(r"decimal\d*\((\d+),\s*(\d+)\)", type_str)
        if match:
            return f"DECIMAL({match.group(1)}, {match.group(2)})"
        return "DECIMAL(38, 18)"

    if "timestamp" in type_str:
        return "DATETIME2"
    if "date" in type_str:
        return "DATE"
    if "time" in type_str:
        return "TIME"
    if "list" in type_str or "struct" in type_str or "map" in type_str:
        return "NVARCHAR(MAX)"

    for key, sql_type in ICEBERG_TO_SQL_TYPE_MAP.items():
        if key in type_str:
            return sql_type

    logger.warning("Unknown Arrow type '%s', defaulting to NVARCHAR(MAX)", type_str)
    return "NVARCHAR(MAX)"


def connect_sql_server(config: dict):
    """Create a connection to SQL Server."""
    sql_cfg = config["sql_server"]
    logger.info(
        "Connecting to SQL Server %s:%s, database: %s",
        sql_cfg["host"],
        sql_cfg.get("port", 1433),
        sql_cfg["database"],
    )
    conn = pymssql.connect(
        server=sql_cfg["host"],
        port=sql_cfg.get("port", 1433),
        user=sql_cfg["user"],
        password=sql_cfg["password"],
        database=sql_cfg["database"],
    )
    logger.info("Connected to SQL Server successfully.")
    return conn


def create_table_if_needed(conn, schema_name: str, table_name: str, arrow_schema, drop_existing: bool = False):
    """Create the target SQL Server table based on the Arrow schema."""
    cursor = conn.cursor()

    full_name = f"[{schema_name}].[{table_name}]"

    # Ensure schema exists
    cursor.execute(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = %s) "
        f"EXEC('CREATE SCHEMA [{schema_name}]')",
        (schema_name,),
    )

    if drop_existing:
        logger.info("Dropping existing table %s if it exists.", full_name)
        cursor.execute(f"IF OBJECT_ID('{schema_name}.{table_name}', 'U') IS NOT NULL DROP TABLE {full_name}")

    columns = []
    for field in arrow_schema:
        sql_type = arrow_type_to_sql(field.type)
        nullable = "NULL" if field.nullable else "NOT NULL"
        columns.append(f"    [{field.name}] {sql_type} {nullable}")

    columns_sql = ",\n".join(columns)
    create_sql = f"""
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}')
BEGIN
    CREATE TABLE {full_name} (
{columns_sql}
    )
END
"""
    logger.info("Ensuring table %s exists.", full_name)
    logger.debug("CREATE TABLE SQL:\n%s", create_sql)
    cursor.execute(create_sql)
    conn.commit()
    logger.info("Table %s is ready.", full_name)


def ingest_data(conn, schema_name: str, table_name: str, arrow_table, batch_size: int = 1000):
    """Insert data from a PyArrow Table into SQL Server in batches."""
    cursor = conn.cursor()
    full_name = f"[{schema_name}].[{table_name}]"
    col_names = [f"[{col}]" for col in arrow_table.column_names]
    placeholders = ", ".join(["%s"] * len(col_names))
    insert_sql = f"INSERT INTO {full_name} ({', '.join(col_names)}) VALUES ({placeholders})"

    total_rows = arrow_table.num_rows
    logger.info("Ingesting %d rows into %s (batch_size=%d)...", total_rows, full_name, batch_size)

    # Convert to Python rows via pandas for reliable type conversion
    df = arrow_table.to_pandas()
    rows = df.values.tolist()

    inserted = 0
    start_time = time.time()

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        inserted += len(batch)
        elapsed = time.time() - start_time
        rate = inserted / elapsed if elapsed > 0 else 0
        logger.info(
            "  Progress: %d / %d rows (%.1f%%) — %.0f rows/sec",
            inserted,
            total_rows,
            (inserted / total_rows) * 100,
            rate,
        )

    elapsed = time.time() - start_time
    logger.info("Ingestion complete: %d rows in %.2f seconds.", inserted, elapsed)


def run(config: dict):
    """Main pipeline: read from Iceberg, write to SQL Server."""
    iceberg_table = get_iceberg_table(config)

    # Apply optional row filter or column selection
    iceberg_cfg = config["iceberg"]
    selected_columns = iceberg_cfg.get("columns")
    row_filter = iceberg_cfg.get("row_filter")

    scan = iceberg_table.scan()
    if selected_columns:
        scan = scan.select(*selected_columns)
        logger.info("Selecting columns: %s", selected_columns)
    if row_filter:
        scan = scan.filter(row_filter)
        logger.info("Applying row filter: %s", row_filter)

    arrow_table = scan.to_arrow()
    logger.info("Read %d rows, %d columns from Iceberg.", arrow_table.num_rows, arrow_table.num_columns)

    if arrow_table.num_rows == 0:
        logger.warning("No data returned from Iceberg table. Nothing to ingest.")
        return

    # SQL Server target settings
    sql_cfg = config["sql_server"]
    target_schema = sql_cfg.get("target_schema", "dbo")
    target_table = sql_cfg.get("target_table", iceberg_cfg["table"])
    drop_existing = sql_cfg.get("drop_existing", False)
    batch_size = sql_cfg.get("batch_size", 1000)

    conn = connect_sql_server(config)
    try:
        create_table_if_needed(conn, target_schema, target_table, arrow_table.schema, drop_existing)
        ingest_data(conn, target_schema, target_table, arrow_table, batch_size)
    finally:
        conn.close()
        logger.info("SQL Server connection closed.")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest data from Apache Iceberg into SQL Server."
    )
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--iceberg-catalog-name",
        help="Override Iceberg catalog name",
    )
    parser.add_argument(
        "--iceberg-namespace",
        help="Override Iceberg namespace",
    )
    parser.add_argument(
        "--iceberg-table",
        help="Override Iceberg table name",
    )
    parser.add_argument(
        "--sql-host",
        help="Override SQL Server host",
    )
    parser.add_argument(
        "--sql-port",
        type=int,
        help="Override SQL Server port",
    )
    parser.add_argument(
        "--sql-database",
        help="Override SQL Server database",
    )
    parser.add_argument(
        "--sql-user",
        help="Override SQL Server user",
    )
    parser.add_argument(
        "--sql-password",
        help="Override SQL Server password",
    )
    parser.add_argument(
        "--sql-target-schema",
        help="Override SQL Server target schema",
    )
    parser.add_argument(
        "--sql-target-table",
        help="Override SQL Server target table name",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Override batch size for inserts",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop the target table before ingestion",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config(args.config)

    # Apply CLI overrides
    if args.iceberg_catalog_name:
        config.setdefault("iceberg", {})["catalog_name"] = args.iceberg_catalog_name
    if args.iceberg_namespace:
        config.setdefault("iceberg", {})["namespace"] = args.iceberg_namespace
    if args.iceberg_table:
        config.setdefault("iceberg", {})["table"] = args.iceberg_table
    if args.sql_host:
        config.setdefault("sql_server", {})["host"] = args.sql_host
    if args.sql_port:
        config.setdefault("sql_server", {})["port"] = args.sql_port
    if args.sql_database:
        config.setdefault("sql_server", {})["database"] = args.sql_database
    if args.sql_user:
        config.setdefault("sql_server", {})["user"] = args.sql_user
    if args.sql_password:
        config.setdefault("sql_server", {})["password"] = args.sql_password
    if args.sql_target_schema:
        config.setdefault("sql_server", {})["target_schema"] = args.sql_target_schema
    if args.sql_target_table:
        config.setdefault("sql_server", {})["target_table"] = args.sql_target_table
    if args.batch_size:
        config.setdefault("sql_server", {})["batch_size"] = args.batch_size
    if args.drop_existing:
        config.setdefault("sql_server", {})["drop_existing"] = True

    run(config)


if __name__ == "__main__":
    main()
