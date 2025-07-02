import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def get_engine():
    """Create SQLAlchemy engine using POSTGRES_* environment variables."""
    load_dotenv()
    db_user = os.getenv("POSTGRES_USER")
    db_pass = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")
    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    )


upsert_sql = text(
    """
    INSERT INTO edinet_documents
        (doc_id, company_id, edinet_code, doc_type_code, submit_date,
         fiscal_year, description, risk_text, risk_count)
    VALUES
        (:doc_id, :company_id, :edinet_code, :doc_type_code, :submit_date,
         :fiscal_year, :description, :risk_text, :risk_count)
    ON CONFLICT (doc_id) DO UPDATE
    SET
        risk_text  = EXCLUDED.risk_text,
        risk_count = EXCLUDED.risk_count;
    """
)


def upsert_risk_data(records):
    """Validate and UPSERT risk_data_records into edinet_documents."""
    required_fields = [
        "doc_id",
        "company_id",
        "edinet_code",
        "doc_type_code",
        "submit_date",
        "fiscal_year",
        "description",
        "risk_text",
        "risk_count",
    ]

    for i, record in enumerate(records):
        for field in required_fields:
            if field not in record:
                raise ValueError(f"Record {i} missing required key: {field}")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)


# Example usage
# risk_data_records = [...]
# upsert_risk_data(risk_data_records)
