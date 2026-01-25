import logging
import re


def upgrade(client):
    """
    Renames _mergestat_synced_at to last_synced for all tables.
    Uses dynamic SHOW CREATE TABLE to preserve schema, then performs
    atomic EXCHANGE TABLES migration.
    """

    logging.info("Starting migration 010: Rename _mergestat_synced_at to last_synced")

    # Get all tables
    try:
        tables_res = client.query("SHOW TABLES")
        all_tables = [row[0] for row in tables_res.result_rows]
    except Exception as e:
        logging.error(f"Failed to list tables: {e}")
        return

    for table in all_tables:
        if table.endswith("_new") or table.endswith("_backup"):
            continue

        # Check if table has _mergestat_synced_at
        try:
            col_res = client.query(
                "SELECT name FROM system.columns WHERE table = {table_name:String} AND name = '_mergestat_synced_at' AND database = currentDatabase()",
                parameters={"table_name": table},
            )
            if not col_res.result_rows:
                # Column doesn't exist, check if last_synced exists
                check_res = client.query(
                    "SELECT name FROM system.columns WHERE table = {table_name:String} AND name = 'last_synced' AND database = currentDatabase()",
                    parameters={"table_name": table},
                )
                if check_res.result_rows:
                    logging.info(f"Table {table} already has last_synced. Skipping.")
                continue
        except Exception as e:
            logging.warning(f"Error checking columns for {table}: {e}")
            continue

        logging.info(f"Migrating table {table}...")

        try:
            # Get Create Statement - use backticks to escape table name
            create_res = client.query(f"SHOW CREATE TABLE `{table}`")
            create_stmt = create_res.result_rows[0][0]

            # Modify Create Statement
            # Replace table name with table_new, handling optional db prefix
            # Regex matches: CREATE TABLE [IF NOT EXISTS] [db.]`?table`? ...
            # We rely on 'table' being the simple name from all_tables loop.
            # Use re.escape on both pattern and replacement to prevent injection
            pattern = re.compile(
                rf"(CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(?:[\w\d_]+\.)?`?){re.escape(table)}(`?\s|`?\()"
            )
            new_create_stmt = pattern.sub(
                rf"\1{re.escape(table)}_new\2", create_stmt, count=1
            )

            if new_create_stmt == create_stmt:
                logging.warning(
                    f"Could not replace table name in create statement for {table}. Stmt: {create_stmt[:100]}..."
                )
                continue

            # Replace _mergestat_synced_at with last_synced
            # This handles both the column definition and the Engine parameter
            new_create_stmt = new_create_stmt.replace(
                "_mergestat_synced_at", "last_synced"
            )

            # Create new table - use backticks to escape table name
            client.command(f"DROP TABLE IF EXISTS `{table}_new`")
            client.command(new_create_stmt)

            # Insert data - use backticks to escape table names
            # Use columns explicitly to be safe?
            # ClickHouse supports * EXCEPT(...) syntax which is robust.
            insert_query = f"""
            INSERT INTO `{table}_new`
            SELECT * EXCEPT(_mergestat_synced_at), _mergestat_synced_at AS last_synced
            FROM `{table}`
            """
            client.command(insert_query)

            # Exchange tables - use backticks to escape table names
            client.command(f"EXCHANGE TABLES `{table}` AND `{table}_new`")

            # Drop old table (now named table_new) - use backticks to escape table name
            client.command(f"DROP TABLE `{table}_new`")

            logging.info(f"Successfully migrated {table}")

        except Exception as e:
            logging.error(f"Failed to migrate table {table}: {e}")
            # Try cleanup; ignore cleanup errors but log them for visibility
            try:
                client.command(f"DROP TABLE IF EXISTS `{table}_new`")
            except Exception as cleanup_err:
                logging.warning(
                    f"Failed to clean up temporary table {table}_new: {cleanup_err}"
                )
            raise
