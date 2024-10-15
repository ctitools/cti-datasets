import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError

import psycopg
from tqdm import tqdm
from psycopg import sql
from psycopg.types.json import Json

from datetime import datetime
from typing import List, Dict, Any, Optional


def validate_json(data):
    """
    Validate a JSON object against the library schema.

    This function loads the JSON schema from 'library.schema.json' and validates
    the provided JSON object against it.

    Args:
        data (dict): The JSON object to validate.

    Returns:
        bool: True if the data is valid.

    Raises:
        jsonschema.exceptions.ValidationError: If the data does not conform to the schema.
        FileNotFoundError: If the schema file is not found.
        json.JSONDecodeError: If the schema file contains invalid JSON.
    """
    SCHEMA_FILE = 'library.schema.json'

    try:
        with open(SCHEMA_FILE, 'r') as schema_file:
            schema = json.load(schema_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"The schema file {SCHEMA_FILE} was not found.")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in the schema file: {e.msg}", e.doc, e.pos)

    validate(instance=data, schema=schema)
    return True


def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO formatted timestamp string to datetime object.

    Supports timestamps with varying precision and timezone formats.

    Args:
        timestamp_str (Optional[str]): The timestamp string.

    Returns:
        Optional[datetime]: The parsed datetime object or None.
    """
    if timestamp_str is None:
        return None
    try:
        # Replace 'Z' with '+00:00' for UTC
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except ValueError:
        # Handle timestamps with higher or lower precision by adjusting microseconds
        try:
            if 'Z' in timestamp_str:
                timestamp_str = timestamp_str.replace("Z", "+00:00")
            main_part, tz = timestamp_str.split('+') if '+' in timestamp_str else timestamp_str.split('-')
            if '.' in main_part:
                main_part, micro = main_part.split('.')
                # Pad microseconds to ensure it has exactly 6 digits
                micro = (micro + '000000')[:6]
                timestamp_fixed = f"{main_part}.{micro}+{tz}" if '+' in timestamp_str else f"{main_part}.{micro}-{tz}"
                return datetime.fromisoformat(timestamp_fixed)
        except Exception:
            pass
        # If all parsing attempts fail, return None or raise an error
        return None


def connect_db() -> psycopg.Connection:
    """
    Establish a connection to the PostgreSQL database.

    Returns:
        psycopg.Connection: The database connection object.
    """
    return psycopg.connect(
        dbname="orkl"
        # user="your_username",
        # password="your_password",
        # host="your_host",
        # port="your_port"
    )


def insert_entry(conn: psycopg.Connection, entry: Dict[str, Any]) -> None:
    """
    Insert an entry into the entries table.

    Args:
        conn (psycopg.Connection): The database connection.
        entry (Dict[str, Any]): The entry data.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO entries (
                id, created_at, updated_at, deleted_at, sha1_hash, title, authors,
                file_creation_date, file_modification_date, file_size, plain_text,
                language, ts_created_at, ts_updated_at, ts_creation_date,
                ts_modification_date, files_pdf, files_text, files_img
            ) VALUES (
                %(id)s, %(created_at)s, %(updated_at)s, %(deleted_at)s, %(sha1_hash)s,
                %(title)s, %(authors)s, %(file_creation_date)s, %(file_modification_date)s,
                %(file_size)s, %(plain_text)s, %(language)s, %(ts_created_at)s,
                %(ts_updated_at)s, %(ts_creation_date)s, %(ts_modification_date)s,
                %(files_pdf)s, %(files_text)s, %(files_img)s
            ) ON CONFLICT (id) DO NOTHING;
            """,
            {
                "id": entry["id"],
                "created_at": parse_timestamp(entry["created_at"]),
                "updated_at": parse_timestamp(entry["updated_at"]),
                "deleted_at": parse_timestamp(entry.get("deleted_at")),
                "sha1_hash": entry["sha1_hash"],
                "title": entry["title"],
                "authors": entry["authors"],
                "file_creation_date": parse_timestamp(entry["file_creation_date"]),
                "file_modification_date": parse_timestamp(entry["file_modification_date"]),
                "file_size": entry["file_size"],
                "plain_text": entry["plain_text"],
                "language": entry["language"],
                "ts_created_at": entry["ts_created_at"],
                "ts_updated_at": entry["ts_updated_at"],
                "ts_creation_date": entry["ts_creation_date"],
                "ts_modification_date": entry["ts_modification_date"],
                "files_pdf": entry.get("files", {}).get("pdf"),
                "files_text": entry.get("files", {}).get("text"),
                "files_img": entry.get("files", {}).get("img"),
            }
        )


def insert_source(conn: psycopg.Connection, source: Dict[str, Any], entry_id: str) -> None:
    """
    Insert a source into the sources table and link it to an entry.

    Args:
        conn (psycopg.Connection): The database connection.
        source (Dict[str, Any]): The source data.
        entry_id (str): The ID of the associated entry.
    """
    with conn.cursor() as cur:
        # Insert into sources
        cur.execute(
            """
            INSERT INTO sources (
                id, created_at, updated_at, deleted_at, name, url, description, reports
            ) VALUES (
                %(id)s, %(created_at)s, %(updated_at)s, %(deleted_at)s, %(name)s,
                %(url)s, %(description)s, %(reports)s
            ) ON CONFLICT (id) DO NOTHING;
            """,
            {
                "id": source["id"],
                "created_at": parse_timestamp(source["created_at"]),
                "updated_at": parse_timestamp(source["updated_at"]),
                "deleted_at": parse_timestamp(source.get("deleted_at")),
                "name": source["name"],
                "url": source["url"],
                "description": source["description"],
                "reports": source.get("reports"),
            }
        )

        # Insert into entries_sources join table
        cur.execute(
            """
            INSERT INTO entries_sources (entry_id, source_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (entry_id, source["id"])
        )


def insert_reference(conn: psycopg.Connection, reference: str, entry_id: str) -> None:
    """
    Insert a reference into the references table.

    Args:
        conn (psycopg.Connection): The database connection.
        reference (str): The reference URL or text.
        entry_id (str): The ID of the associated entry.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO "references" (entry_id, reference)
            VALUES (%s, %s);
            """,
            (entry_id, reference)
        )


def insert_report_name(conn: psycopg.Connection, report_name: str, entry_id: str) -> None:
    """
    Insert a report name into the report_names table.

    Args:
        conn (psycopg.Connection): The database connection.
        report_name (str): The name of the report.
        entry_id (str): The ID of the associated entry.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO report_names (entry_id, report_name)
            VALUES (%s, %s);
            """,
            (entry_id, report_name)
        )


def insert_threat_actor(conn: psycopg.Connection, threat_actor: Dict[str, Any], entry_id: str) -> None:
    """
    Insert a threat actor into the threat_actors table and link it to an entry.

    Args:
        conn (psycopg.Connection): The database connection.
        threat_actor (Dict[str, Any]): The threat actor data.
        entry_id (str): The ID of the associated entry.
    """
    with conn.cursor() as cur:
        # Insert into threat_actors
        cur.execute(
            """
            INSERT INTO threat_actors (
                id, created_at, updated_at, deleted_at, main_name, source_name,
                tools, source_id, reports
            ) VALUES (
                %(id)s, %(created_at)s, %(updated_at)s, %(deleted_at)s, %(main_name)s,
                %(source_name)s, %(tools)s, %(source_id)s, %(reports)s
            ) ON CONFLICT (id) DO NOTHING;
            """,
            {
                "id": threat_actor["id"],
                "created_at": parse_timestamp(threat_actor["created_at"]),
                "updated_at": parse_timestamp(threat_actor["updated_at"]),
                "deleted_at": parse_timestamp(threat_actor.get("deleted_at")),
                "main_name": threat_actor["main_name"],
                "source_name": threat_actor["source_name"],
                "tools": threat_actor.get("tools") or [],
                "source_id": threat_actor["source_id"],
                "reports": threat_actor.get("reports"),
            }
        )

        # Insert aliases if they exist and are not null
        aliases = threat_actor.get("aliases") or []
        for alias in aliases:
            cur.execute(
                """
                INSERT INTO threat_actors_aliases (threat_actor_id, alias)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (threat_actor["id"], alias)
            )

        # Insert into entries_threat_actors join table
        cur.execute(
            """
            INSERT INTO entries_threat_actors (entry_id, threat_actor_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (entry_id, threat_actor["id"])
        )


def process_entry(conn: psycopg.Connection, entry: Dict[str, Any]) -> None:
    """
    Process and insert a single entry along with its related data.

    Args:
        conn (psycopg.Connection): The database connection.
        entry (Dict[str, Any]): The entry data.
    """
    insert_entry(conn, entry)

    # Insert sources if they exist and are not null
    sources = entry.get("sources") or []
    for source in sources:
        insert_source(conn, source, entry["id"])

    # Insert references if they exist and are not null
    references = entry.get("references") or []
    for reference in references:
        insert_reference(conn, reference, entry["id"])

    # Insert report names if they exist and are not null
    report_names = entry.get("report_names") or []
    for report_name in report_names:
        insert_report_name(conn, report_name, entry["id"])

    # Insert threat actors if they exist and are not null
    threat_actors = entry.get("threat_actors") or []
    for threat_actor in threat_actors:
        insert_threat_actor(conn, threat_actor, entry["id"])


def main():
    """
    Main function to read JSON data and insert it into the PostgreSQL database.
    """
    # Load JSON data
    with open("library.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print("No data to insert.")
        return

    # next validate the data against the schema 'library.schema.json'
    print("Validating data...(this might take a while)")
    is_valid = validate_json(data)
    if is_valid:
        print("Data is valid.")
    else:
        print("Data is not valid.")
        return

    print("Inserting data into the database...")
    # Connect to the database
    conn = connect_db()

    try:
        with conn:
            i = 1
            skipped = 0
            for entry in tqdm(data):
                try:
                    process_entry(conn, entry)
                except Exception as e:
                    print(f"An error occurred at element {i}: {e}")
                    print(80 * "-")
                    print(json.dumps(entry, indent=4))
                    print(80 * "-")
                    skipped += 1
                i += 1
        print(f"Data inserted successfully. {i} entries inserted. {skipped} entries skipped.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        conn.close()
        print("DONE")

if __name__ == "__main__":
    main()