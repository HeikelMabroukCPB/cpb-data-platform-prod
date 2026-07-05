from __future__ import annotations

import hashlib
import imaplib
import io
import json
import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from email import policy
from email.message import Message
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from typing import Any

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from shared.bq import get_bq_client, load_dataframe_in_chunks
from shared.mail import send_email
from shared.metadata import log_pipeline_run
from shared.utils import validate_common_config


logger = logging.getLogger(__name__)


# ============================================================
# General configuration
# ============================================================

PROJECT_ID = os.environ.get(
    "PROJECT_ID",
    "cpb-data-platform-prod",
)

DATASET_RAW = os.environ.get(
    "DATASET_RAW",
    "cpb_raw",
)

DATASET_META = os.environ.get(
    "DATASET_META",
    "cpb_meta",
)

MODULE_NAME = os.environ.get(
    "MODULE_NAME",
    "gmail_ingestion.conversion_tracking",
)

PIPELINE_NAME = os.environ.get(
    "PIPELINE_NAME",
    "gmail_conversion_tracking",
)

TABLE_NAME = os.environ.get(
    "TABLE_NAME",
    "conversion_tracking",
)

SOURCE_SYSTEM = os.environ.get(
    "SOURCE_SYSTEM",
    "gmail_conversion_tracking",
)

CHUNK_SIZE = int(
    os.environ.get("CHUNK_SIZE", 10000)
)


# ============================================================
# Gmail configuration
# ============================================================

# These fallbacks let you reuse the variables already used
# by your outgoing email functionality.
GMAIL_EMAIL = (
    os.environ.get("GMAIL_EMAIL")
    or os.environ.get("SMTP_USER")
    or os.environ.get("EMAIL_SENDER")
)

GMAIL_APP_PASSWORD = (
    os.environ.get("GMAIL_APP_PASSWORD")
    or os.environ.get("SMTP_PASSWORD")
    or os.environ.get("EMAIL_PASSWORD")
)

GMAIL_EXPECTED_SENDER = os.environ.get(
    "GMAIL_EXPECTED_SENDER",
    "heikel.mabrouk@carepersonalbeauty.be",
)

GMAIL_EXPECTED_SUBJECT = os.environ.get(
    "GMAIL_EXPECTED_SUBJECT",
    "Alert: Conversion Tracking has results",
)

GMAIL_FOLDER = os.environ.get(
    "GMAIL_FOLDER",
    "INBOX",
)

GMAIL_SEARCH_DAYS = int(
    os.environ.get("GMAIL_SEARCH_DAYS", 30)
)

GMAIL_MAX_EMAILS = int(
    os.environ.get("GMAIL_MAX_EMAILS", 100)
)

IMAP_HOST = os.environ.get(
    "GMAIL_IMAP_HOST",
    "imap.gmail.com",
)

IMAP_PORT = int(
    os.environ.get("GMAIL_IMAP_PORT", 993)
)

IMAP_TIMEOUT = int(
    os.environ.get("GMAIL_IMAP_TIMEOUT", 60)
)


# ============================================================
# CSV configuration
# ============================================================

# Use "auto" to automatically detect comma, semicolon, tab, etc.
CSV_DELIMITER = os.environ.get(
    "CSV_DELIMITER",
    "auto",
)

# Use "auto" to try utf-8-sig, utf-8, cp1252 and latin-1.
CSV_ENCODING = os.environ.get(
    "CSV_ENCODING",
    "auto",
)


# ============================================================
# BigQuery destinations
# ============================================================

RAW_TABLE = (
    f"{PROJECT_ID}.{DATASET_RAW}."
    f"{SOURCE_SYSTEM}_{TABLE_NAME}"
)

META_TABLE = (
    f"{PROJECT_ID}.{DATASET_META}.pipeline_runs"
)

FILE_LOG_TABLE = (
    f"{PROJECT_ID}.{DATASET_META}."
    f"email_file_ingestion"
)


# ============================================================
# Metadata columns
# ============================================================

METADATA_SCHEMA = [
    bigquery.SchemaField(
        "source_message_id",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_gmail_uid",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_attachment_index",
        "INTEGER",
    ),
    bigquery.SchemaField(
        "source_filename",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_file_hash",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_row_number",
        "INTEGER",
    ),
    bigquery.SchemaField(
        "source_email_from",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_email_subject",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_email_received_at",
        "TIMESTAMP",
    ),
    bigquery.SchemaField(
        "source_column_map_json",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_row_json",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_system",
        "STRING",
    ),
    bigquery.SchemaField(
        "run_id",
        "STRING",
    ),
    bigquery.SchemaField(
        "load_timestamp",
        "TIMESTAMP",
    ),
    bigquery.SchemaField(
        "load_date",
        "DATE",
    ),
    bigquery.SchemaField(
        "record_hash",
        "STRING",
    ),
]

METADATA_COLUMN_NAMES = {
    field.name
    for field in METADATA_SCHEMA
}


FILE_LOG_SCHEMA = [
    bigquery.SchemaField(
        "pipeline_name",
        "STRING",
    ),
    bigquery.SchemaField(
        "source_system",
        "STRING",
    ),
    bigquery.SchemaField(
        "gmail_uid",
        "STRING",
    ),
    bigquery.SchemaField(
        "message_id",
        "STRING",
    ),
    bigquery.SchemaField(
        "attachment_index",
        "INTEGER",
    ),
    bigquery.SchemaField(
        "filename",
        "STRING",
    ),
    bigquery.SchemaField(
        "file_hash",
        "STRING",
    ),
    bigquery.SchemaField(
        "email_received_at",
        "TIMESTAMP",
    ),
    bigquery.SchemaField(
        "status",
        "STRING",
    ),
    bigquery.SchemaField(
        "rows_loaded",
        "INTEGER",
    ),
    bigquery.SchemaField(
        "raw_table",
        "STRING",
    ),
    bigquery.SchemaField(
        "run_id",
        "STRING",
    ),
    bigquery.SchemaField(
        "processed_at",
        "TIMESTAMP",
    ),
    bigquery.SchemaField(
        "error_message",
        "STRING",
    ),
]


# ============================================================
# Configuration validation
# ============================================================

def validate_config() -> None:
    validate_common_config(
        {
            "PROJECT_ID": PROJECT_ID,
            "DATASET_RAW": DATASET_RAW,
            "DATASET_META": DATASET_META,
            "MODULE_NAME": MODULE_NAME,
            "PIPELINE_NAME": PIPELINE_NAME,
            "TABLE_NAME": TABLE_NAME,
            "SOURCE_SYSTEM": SOURCE_SYSTEM,
            "GMAIL_EMAIL": GMAIL_EMAIL,
            "GMAIL_APP_PASSWORD": GMAIL_APP_PASSWORD,
            "GMAIL_EXPECTED_SENDER": GMAIL_EXPECTED_SENDER,
            "GMAIL_EXPECTED_SUBJECT": GMAIL_EXPECTED_SUBJECT,
        }
    )

    if GMAIL_SEARCH_DAYS < 0:
        raise ValueError(
            "GMAIL_SEARCH_DAYS cannot be negative"
        )

    if GMAIL_MAX_EMAILS <= 0:
        raise ValueError(
            "GMAIL_MAX_EMAILS must be greater than zero"
        )

    if CHUNK_SIZE <= 0:
        raise ValueError(
            "CHUNK_SIZE must be greater than zero"
        )


# ============================================================
# General helpers
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def normalize_bigquery_type(
    field_type: str,
) -> str:
    aliases = {
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
    }

    normalized = field_type.upper()

    return aliases.get(
        normalized,
        normalized,
    )


def sanitize_column_name(
    value: Any,
) -> str:
    """
    Convert a CSV column name into a safe BigQuery column name.
    """

    text = str(value).strip()

    text = unicodedata.normalize(
        "NFKD",
        text,
    )

    text = text.encode(
        "ascii",
        "ignore",
    ).decode("ascii")

    text = text.lower()

    text = re.sub(
        r"[^a-z0-9_]+",
        "_",
        text,
    )

    text = re.sub(
        r"_+",
        "_",
        text,
    )

    text = text.strip("_")

    if not text:
        text = "column"

    if text[0].isdigit():
        text = f"column_{text}"

    if text in METADATA_COLUMN_NAMES:
        text = f"csv_{text}"

    return text[:300]


def create_unique_column_names(
    columns: list[Any],
) -> tuple[list[str], str]:
    """
    Sanitize CSV headers and make duplicated names unique.
    """

    used_names = set(METADATA_COLUMN_NAMES)
    sanitized_columns: list[str] = []
    column_mapping: list[dict[str, str]] = []

    for original_column in columns:
        base_name = sanitize_column_name(
            original_column
        )

        candidate_name = base_name
        sequence = 2

        while candidate_name in used_names:
            candidate_name = (
                f"{base_name}_{sequence}"
            )
            sequence += 1

        used_names.add(candidate_name)
        sanitized_columns.append(candidate_name)

        column_mapping.append(
            {
                "original": str(original_column),
                "sanitized": candidate_name,
            }
        )

    mapping_json = json.dumps(
        column_mapping,
        ensure_ascii=False,
        sort_keys=True,
    )

    return sanitized_columns, mapping_json


# ============================================================
# Gmail connection
# ============================================================

def connect_to_gmail() -> imaplib.IMAP4_SSL:
    logger.info(
        "Connecting to Gmail through IMAP | account=%s",
        GMAIL_EMAIL,
    )

    mailbox = imaplib.IMAP4_SSL(
        host=IMAP_HOST,
        port=IMAP_PORT,
        timeout=IMAP_TIMEOUT,
    )

    # Google sometimes displays app passwords with spaces.
    # IMAP authentication requires the password without spaces.
    app_password = (
        GMAIL_APP_PASSWORD
        .replace(" ", "")
        .strip()
    )

    mailbox.login(
        GMAIL_EMAIL,
        app_password,
    )

    status, response = mailbox.select(
        mailbox=GMAIL_FOLDER,
        readonly=True,
    )

    if status != "OK":
        raise RuntimeError(
            f"Could not select Gmail folder "
            f"'{GMAIL_FOLDER}': {response}"
        )

    logger.info(
        "Gmail connection established | folder=%s",
        GMAIL_FOLDER,
    )

    return mailbox


def close_gmail_connection(
    mailbox: imaplib.IMAP4_SSL | None,
) -> None:
    if mailbox is None:
        return

    try:
        mailbox.close()
    except Exception:
        logger.warning(
            "Could not close selected Gmail folder",
            exc_info=True,
        )

    try:
        mailbox.logout()
    except Exception:
        logger.warning(
            "Could not log out from Gmail",
            exc_info=True,
        )


# ============================================================
# Gmail search and extraction
# ============================================================

def build_gmail_search_query() -> str:
    query_parts = [
        f"from:{GMAIL_EXPECTED_SENDER}",
        (
            f'subject:"'
            f'{GMAIL_EXPECTED_SUBJECT}'
            f'"'
        ),
        "has:attachment",
        "filename:csv",
    ]

    if GMAIL_SEARCH_DAYS > 0:
        query_parts.append(
            f"newer_than:{GMAIL_SEARCH_DAYS}d"
        )

    return " ".join(query_parts)


def find_matching_email_uids(
    mailbox: imaplib.IMAP4_SSL,
) -> list[str]:
    query = build_gmail_search_query()

    logger.info(
        "Searching Gmail | query=%s",
        query,
    )

    escaped_query = (
        query
        .replace("\\", "\\\\")
        .replace('"', '\\"')
    )

    # X-GM-RAW allows Gmail search syntax through IMAP.
    status, response = mailbox.uid(
        "search",
        None,
        "X-GM-RAW",
        f'"{escaped_query}"',
    )

    if status != "OK":
        raise RuntimeError(
            f"Gmail IMAP search failed: {response}"
        )

    if not response or not response[0]:
        logger.info(
            "No matching Gmail messages found"
        )
        return []

    uids = [
        value.decode("utf-8")
        for value in response[0].split()
    ]

    # UID values are numeric, so this sorts oldest to newest.
    uids = sorted(
        uids,
        key=int,
    )

    # Keep the newest N messages.
    if len(uids) > GMAIL_MAX_EMAILS:
        uids = uids[-GMAIL_MAX_EMAILS:]

    logger.info(
        "Matching Gmail messages found | count=%s",
        len(uids),
    )

    return uids


def fetch_email_message(
    mailbox: imaplib.IMAP4_SSL,
    gmail_uid: str,
) -> Message:
    status, response = mailbox.uid(
        "fetch",
        gmail_uid,
        "(BODY.PEEK[])",
    )

    if status != "OK":
        raise RuntimeError(
            f"Could not retrieve Gmail UID "
            f"{gmail_uid}: {response}"
        )

    raw_message: bytes | None = None

    for response_part in response:
        if (
            isinstance(response_part, tuple)
            and len(response_part) >= 2
            and isinstance(response_part[1], bytes)
        ):
            raw_message = response_part[1]
            break

    if raw_message is None:
        raise ValueError(
            f"No email content returned for "
            f"Gmail UID {gmail_uid}"
        )

    return BytesParser(
        policy=policy.default,
    ).parsebytes(raw_message)


def parse_email_received_at(
    message: Message,
) -> datetime | None:
    date_header = message.get("Date")

    if not date_header:
        return None

    try:
        parsed_date = parsedate_to_datetime(
            date_header
        )

        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(
                tzinfo=timezone.utc
            )

        return parsed_date.astimezone(
            timezone.utc
        )

    except Exception:
        logger.warning(
            "Could not parse email Date header: %s",
            date_header,
        )
        return None


def extract_csv_attachments(
    message: Message,
    gmail_uid: str,
) -> list[dict[str, Any]]:
    message_id = (
        message.get("Message-ID")
        or f"gmail-imap-uid:{gmail_uid}"
    )

    email_from = str(
        message.get("From") or ""
    )

    email_subject = str(
        message.get("Subject") or ""
    )

    received_at = parse_email_received_at(
        message
    )

    attachments: list[dict[str, Any]] = []
    attachment_index = 0

    for part in message.walk():
        if part.is_multipart():
            continue

        filename = part.get_filename()

        if not filename:
            continue

        if not filename.lower().endswith(".csv"):
            continue

        content = part.get_payload(
            decode=True
        )

        if not content:
            logger.warning(
                "Empty CSV attachment skipped | "
                "gmail_uid=%s | filename=%s",
                gmail_uid,
                filename,
            )
            continue

        attachment_index += 1

        attachments.append(
            {
                "gmail_uid": gmail_uid,
                "message_id": message_id,
                "attachment_index": attachment_index,
                "filename": filename,
                "content": content,
                "file_hash": sha256_bytes(content),
                "email_from": email_from,
                "email_subject": email_subject,
                "email_received_at": received_at,
            }
        )

    logger.info(
        "CSV attachments extracted | "
        "gmail_uid=%s | attachment_count=%s",
        gmail_uid,
        len(attachments),
    )

    return attachments


# ============================================================
# CSV parsing
# ============================================================

def decode_csv_content(
    content: bytes,
) -> tuple[str, str]:
    if CSV_ENCODING.lower() != "auto":
        return (
            content.decode(CSV_ENCODING),
            CSV_ENCODING,
        )

    candidate_encodings = [
        "utf-8-sig",
        "utf-8",
        "cp1252",
        "latin-1",
    ]

    last_error: Exception | None = None

    for encoding in candidate_encodings:
        try:
            return (
                content.decode(encoding),
                encoding,
            )

        except UnicodeDecodeError as error:
            last_error = error

    raise ValueError(
        "Could not decode CSV attachment"
    ) from last_error


def read_csv_attachment(
    content: bytes,
) -> tuple[pd.DataFrame, str]:
    csv_text, detected_encoding = (
        decode_csv_content(content)
    )

    logger.info(
        "Reading CSV attachment | encoding=%s",
        detected_encoding,
    )

    if CSV_DELIMITER.lower() == "auto":
        dataframe = pd.read_csv(
            io.StringIO(csv_text),
            sep=None,
            engine="python",
            dtype=str,
            keep_default_na=False,
            on_bad_lines="error",
        )
    else:
        dataframe = pd.read_csv(
            io.StringIO(csv_text),
            sep=CSV_DELIMITER,
            dtype=str,
            keep_default_na=False,
            on_bad_lines="error",
        )

    if len(dataframe.columns) == 0:
        raise ValueError(
            "CSV attachment contains no columns"
        )

    sanitized_columns, column_mapping_json = (
        create_unique_column_names(
            list(dataframe.columns)
        )
    )

    dataframe.columns = sanitized_columns

    # Keep the raw layer as strings.
    dataframe = dataframe.astype("string")

    dataframe = dataframe.replace(
        {
            "": pd.NA,
        }
    )

    logger.info(
        "CSV parsed | rows=%s | columns=%s",
        len(dataframe),
        list(dataframe.columns),
    )

    return dataframe, column_mapping_json


# ============================================================
# Data transformation
# ============================================================

def transform_attachment_dataframe(
    dataframe: pd.DataFrame,
    attachment: dict[str, Any],
    column_mapping_json: str,
    run_id: str,
) -> pd.DataFrame:
    transformed_df = dataframe.copy()

    source_columns = list(
        transformed_df.columns
    )

    load_timestamp = utc_now()
    load_date = load_timestamp.date()

    transformed_df.insert(
        len(transformed_df.columns),
        "source_message_id",
        attachment["message_id"],
    )

    transformed_df[
        "source_gmail_uid"
    ] = attachment["gmail_uid"]

    transformed_df[
        "source_attachment_index"
    ] = attachment["attachment_index"]

    transformed_df[
        "source_filename"
    ] = attachment["filename"]

    transformed_df[
        "source_file_hash"
    ] = attachment["file_hash"]

    transformed_df[
        "source_row_number"
    ] = range(
        1,
        len(transformed_df) + 1,
    )

    transformed_df[
        "source_email_from"
    ] = attachment["email_from"]

    transformed_df[
        "source_email_subject"
    ] = attachment["email_subject"]

    transformed_df[
        "source_email_received_at"
    ] = attachment["email_received_at"]

    transformed_df[
        "source_column_map_json"
    ] = column_mapping_json

    transformed_df["source_row_json"] = (
        transformed_df[source_columns]
        .apply(
            lambda row: json.dumps(
                {
                    column: (
                        None
                        if pd.isna(value)
                        else str(value)
                    )
                    for column, value
                    in row.items()
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            axis=1,
        )
    )

    transformed_df[
        "source_system"
    ] = SOURCE_SYSTEM

    transformed_df[
        "run_id"
    ] = run_id

    transformed_df[
        "load_timestamp"
    ] = load_timestamp

    transformed_df[
        "load_date"
    ] = load_date

    transformed_df["record_hash"] = (
        transformed_df.apply(
            lambda row: sha256_text(
                json.dumps(
                    {
                        "file_hash": (
                            row["source_file_hash"]
                        ),
                        "row_number": (
                            int(
                                row[
                                    "source_row_number"
                                ]
                            )
                        ),
                        "row": {
                            column: (
                                None
                                if pd.isna(
                                    row[column]
                                )
                                else str(
                                    row[column]
                                )
                            )
                            for column
                            in source_columns
                        },
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            ),
            axis=1,
        )
    )

    logger.info(
        "Attachment transformation complete | "
        "filename=%s | rows=%s",
        attachment["filename"],
        len(transformed_df),
    )

    return transformed_df


def build_raw_table_schema(
    dataframe: pd.DataFrame,
) -> list[bigquery.SchemaField]:
    source_columns = [
        column
        for column in dataframe.columns
        if column not in METADATA_COLUMN_NAMES
    ]

    source_schema = [
        bigquery.SchemaField(
            column,
            "STRING",
        )
        for column in source_columns
    ]

    return source_schema + METADATA_SCHEMA


# ============================================================
# BigQuery table management
# ============================================================

def ensure_raw_table(
    client: bigquery.Client,
    schema: list[bigquery.SchemaField],
) -> None:
    try:
        table = client.get_table(
            RAW_TABLE
        )

    except NotFound:
        logger.info(
            "Creating raw table: %s",
            RAW_TABLE,
        )

        table = bigquery.Table(
            RAW_TABLE,
            schema=schema,
        )

        table.time_partitioning = (
            bigquery.TimePartitioning(
                type_=(
                    bigquery.TimePartitioningType.DAY
                ),
                field="load_date",
            )
        )

        table.clustering_fields = [
            "source_file_hash",
            "source_message_id",
        ]

        client.create_table(table)

        logger.info(
            "Raw table created: %s",
            RAW_TABLE,
        )

        return

    existing_fields = {
        field.name: normalize_bigquery_type(
            field.field_type
        )
        for field in table.schema
    }

    for field in schema:
        expected_type = normalize_bigquery_type(
            field.field_type
        )

        if field.name not in existing_fields:
            logger.info(
                "Adding column to raw table | "
                "column=%s | type=%s",
                field.name,
                expected_type,
            )

            alter_query = (
                f"ALTER TABLE `{RAW_TABLE}` "
                f"ADD COLUMN `{field.name}` "
                f"{expected_type}"
            )

            client.query(
                alter_query
            ).result()

            continue

        existing_type = existing_fields[
            field.name
        ]

        if existing_type != expected_type:
            raise ValueError(
                f"Schema mismatch for column "
                f"'{field.name}': "
                f"existing={existing_type}, "
                f"expected={expected_type}"
            )


def ensure_file_log_table(
    client: bigquery.Client,
) -> None:
    try:
        client.get_table(
            FILE_LOG_TABLE
        )
        return

    except NotFound:
        logger.info(
            "Creating email ingestion log table: %s",
            FILE_LOG_TABLE,
        )

    table = bigquery.Table(
        FILE_LOG_TABLE,
        schema=FILE_LOG_SCHEMA,
    )

    table.time_partitioning = (
        bigquery.TimePartitioning(
            type_=(
                bigquery.TimePartitioningType.DAY
            ),
            field="processed_at",
        )
    )

    table.clustering_fields = [
        "file_hash",
        "status",
        "pipeline_name",
    ]

    try:
        client.create_table(table)

    except Exception:
        # Another parallel execution may have created it.
        client.get_table(
            FILE_LOG_TABLE
        )


def attachment_already_processed(
    client: bigquery.Client,
    file_hash: str,
) -> bool:
    query = f"""
        SELECT 1
        FROM `{FILE_LOG_TABLE}`
        WHERE pipeline_name = @pipeline_name
          AND file_hash = @file_hash
          AND status = 'SUCCESS'
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "pipeline_name",
                "STRING",
                PIPELINE_NAME,
            ),
            bigquery.ScalarQueryParameter(
                "file_hash",
                "STRING",
                file_hash,
            ),
        ]
    )

    rows = list(
        client.query(
            query,
            job_config=job_config,
        ).result()
    )

    return len(rows) > 0


def write_file_log(
    client: bigquery.Client,
    attachment: dict[str, Any],
    status: str,
    run_id: str,
    rows_loaded: int,
    error_message: str | None = None,
) -> None:
    row = {
        "pipeline_name": PIPELINE_NAME,
        "source_system": SOURCE_SYSTEM,
        "gmail_uid": attachment["gmail_uid"],
        "message_id": attachment["message_id"],
        "attachment_index": (
            attachment["attachment_index"]
        ),
        "filename": attachment["filename"],
        "file_hash": attachment["file_hash"],
        "email_received_at": (
            attachment["email_received_at"]
            .isoformat()
            if attachment[
                "email_received_at"
            ]
            else None
        ),
        "status": status,
        "rows_loaded": rows_loaded,
        "raw_table": RAW_TABLE,
        "run_id": run_id,
        "processed_at": utc_now().isoformat(),
        "error_message": error_message,
    }

    errors = client.insert_rows_json(
        FILE_LOG_TABLE,
        [row],
    )

    if errors:
        raise RuntimeError(
            f"Could not write email ingestion "
            f"log: {errors}"
        )


# ============================================================
# Attachment loading
# ============================================================

def process_attachment(
    client: bigquery.Client,
    attachment: dict[str, Any],
    run_id: str,
) -> int:
    file_hash = attachment["file_hash"]
    filename = attachment["filename"]

    if attachment_already_processed(
        client=client,
        file_hash=file_hash,
    ):
        logger.info(
            "Attachment already processed, skipping | "
            "filename=%s | file_hash=%s",
            filename,
            file_hash,
        )

        return 0

    write_file_log(
        client=client,
        attachment=attachment,
        status="RECEIVED",
        run_id=run_id,
        rows_loaded=0,
    )

    try:
        raw_df, column_mapping_json = (
            read_csv_attachment(
                attachment["content"]
            )
        )

        if raw_df.empty:
            logger.warning(
                "CSV contains no data rows | "
                "filename=%s",
                filename,
            )

            write_file_log(
                client=client,
                attachment=attachment,
                status="SUCCESS",
                run_id=run_id,
                rows_loaded=0,
            )

            return 0

        transformed_df = (
            transform_attachment_dataframe(
                dataframe=raw_df,
                attachment=attachment,
                column_mapping_json=(
                    column_mapping_json
                ),
                run_id=run_id,
            )
        )

        table_schema = build_raw_table_schema(
            transformed_df
        )

        ensure_raw_table(
            client=client,
            schema=table_schema,
        )

        load_dataframe_in_chunks(
            client=client,
            df=transformed_df,
            table_id=RAW_TABLE,
            schema=table_schema,
            chunk_size=CHUNK_SIZE,
        )

        rows_loaded = len(
            transformed_df
        )

        write_file_log(
            client=client,
            attachment=attachment,
            status="SUCCESS",
            run_id=run_id,
            rows_loaded=rows_loaded,
        )

        logger.info(
            "Attachment loaded successfully | "
            "filename=%s | rows_loaded=%s",
            filename,
            rows_loaded,
        )

        return rows_loaded

    except Exception as error:
        try:
            write_file_log(
                client=client,
                attachment=attachment,
                status="FAILED",
                run_id=run_id,
                rows_loaded=0,
                error_message=str(error),
            )

        except Exception as log_error:
            logger.error(
                "Could not write failed attachment "
                "log: %s",
                log_error,
            )

        raise


# ============================================================
# Failure handling
# ============================================================

def handle_pipeline_failure(
    client: bigquery.Client,
    run_id: str,
    started_at: datetime,
    error: Exception,
):
    finished_at = utc_now()

    try:
        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="FAILED",
            rows_loaded=0,
            started_at=started_at,
            finished_at=finished_at,
            message=(
                f"module_name={MODULE_NAME} | "
                f"{str(error)}"
            ),
        )

    except Exception as log_error:
        logger.error(
            "Could not log failed pipeline run: %s",
            log_error,
        )

    send_email(
        subject=(
            f"❌ {PIPELINE_NAME} pipeline failed"
        ),
        body=(
            f"Pipeline: {PIPELINE_NAME}\n"
            f"Module name: {MODULE_NAME}\n"
            f"Run ID: {run_id}\n"
            f"Time: {finished_at}\n"
            f"Raw table: {RAW_TABLE}\n"
            f"Error: {str(error)}"
        ),
    )

    logger.exception(
        "Pipeline failed | run_id=%s",
        run_id,
    )

    return (
        f"Pipeline failed: {str(error)}",
        500,
    )


# ============================================================
# Main ETL
# ============================================================

def run_etl():
    client = get_bq_client()

    run_id = utc_now().strftime(
        "%Y%m%d_%H%M%S"
    )

    started_at = utc_now()

    mailbox: imaplib.IMAP4_SSL | None = None

    logger.info(
        "Pipeline started | module_name=%s | "
        "pipeline=%s | run_id=%s",
        MODULE_NAME,
        PIPELINE_NAME,
        run_id,
    )

    logger.info(
        "Target raw table: %s",
        RAW_TABLE,
    )

    try:
        validate_config()

        ensure_file_log_table(
            client
        )

        mailbox = connect_to_gmail()

        gmail_uids = (
            find_matching_email_uids(
                mailbox
            )
        )

        total_rows_loaded = 0
        attachments_found = 0
        attachments_skipped = 0

        for gmail_uid in gmail_uids:
            message = fetch_email_message(
                mailbox=mailbox,
                gmail_uid=gmail_uid,
            )

            attachments = (
                extract_csv_attachments(
                    message=message,
                    gmail_uid=gmail_uid,
                )
            )

            for attachment in attachments:
                attachments_found += 1

                already_processed = (
                    attachment_already_processed(
                        client=client,
                        file_hash=(
                            attachment[
                                "file_hash"
                            ]
                        ),
                    )
                )

                if already_processed:
                    attachments_skipped += 1

                    logger.info(
                        "Skipping previously loaded "
                        "attachment | filename=%s",
                        attachment["filename"],
                    )

                    continue

                rows_loaded = (
                    process_attachment(
                        client=client,
                        attachment=attachment,
                        run_id=run_id,
                    )
                )

                total_rows_loaded += rows_loaded

        finished_at = utc_now()

        log_pipeline_run(
            client=client,
            meta_table=META_TABLE,
            pipeline_name=PIPELINE_NAME,
            run_id=run_id,
            status="SUCCESS",
            rows_loaded=total_rows_loaded,
            started_at=started_at,
            finished_at=finished_at,
            message=(
                f"Pipeline succeeded | "
                f"module_name={MODULE_NAME} | "
                f"emails_found={len(gmail_uids)} | "
                f"attachments_found="
                f"{attachments_found} | "
                f"attachments_skipped="
                f"{attachments_skipped}"
            ),
        )

        logger.info(
            "Pipeline finished successfully | "
            "emails_found=%s | "
            "attachments_found=%s | "
            "attachments_skipped=%s | "
            "rows_loaded=%s | run_id=%s",
            len(gmail_uids),
            attachments_found,
            attachments_skipped,
            total_rows_loaded,
            run_id,
        )

        return (
            f"{total_rows_loaded} rows loaded into "
            f"{RAW_TABLE}",
            200,
        )

    except Exception as error:
        return handle_pipeline_failure(
            client=client,
            run_id=run_id,
            started_at=started_at,
            error=error,
        )

    finally:
        close_gmail_connection(
            mailbox
        )