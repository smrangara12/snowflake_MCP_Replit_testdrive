import os
import snowflake.connector

def _clean_account(account: str) -> str:
    suffix = ".snowflakecomputing.com"
    if account.lower().endswith(suffix):
        account = account[: -len(suffix)]
    return account

def get_connection():
    return snowflake.connector.connect(
        account=_clean_account(os.environ["SNOWFLAKE_ACCOUNT"]),
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", ""),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", ""),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )

def run_sql_file(cursor, filepath):
    with open(filepath) as f:
        raw = f.read()
    statements = raw.split(";")
    for stmt in statements:
        # Strip comment lines and whitespace
        lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if not clean:
            continue
        cursor.execute(clean)
        print(f"  OK: {clean[:80].replace(chr(10), ' ')}...")

def print_table(cursor, query, title):
    cursor.execute(query)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    print("  " + " | ".join(f"{c:<28}" for c in cols))
    print("  " + "-" * (31 * len(cols)))
    for row in rows:
        print("  " + " | ".join(f"{str(v):<28}" for v in row))

def main():
    conn = get_connection()
    cur = conn.cursor()

    try:
        # Use or create a database/schema
        cur.execute("CREATE DATABASE IF NOT EXISTS cpt_demo")
        cur.execute("USE DATABASE cpt_demo")
        cur.execute("CREATE SCHEMA IF NOT EXISTS medical")
        cur.execute("USE SCHEMA medical")

        print("\nRunning setup SQL...")
        run_sql_file(cur, "cpt_coding_setup.sql")
        print("\nSetup complete.\n")

        # Show tables
        print_table(cur,
            "SELECT * FROM cpt_codes ORDER BY cpt_code",
            "CPT Codes")

        print_table(cur,
            "SELECT patient_id, first_name, last_name, date_of_birth, age, gender, insurance_id FROM patients",
            "Patients")

        print_table(cur,
            "SELECT visit_id, patient_id, visit_date, visit_type, provider_name, diagnosis_code FROM visits",
            "Visits")

        print_table(cur,
            "SELECT vp.procedure_id, vp.visit_id, vp.cpt_code, c.description, vp.units, vp.fee_charged, vp.reimbursement "
            "FROM visit_procedures vp JOIN cpt_codes c ON vp.cpt_code = c.cpt_code ORDER BY vp.procedure_id",
            "Visit Procedures")

        # Summary
        print_table(cur,
            """
            SELECT
                p.first_name || ' ' || p.last_name   AS patient,
                p.age,
                v.visit_date,
                v.visit_type,
                v.provider_name,
                vp.cpt_code,
                c.description,
                vp.fee_charged,
                vp.reimbursement
            FROM visit_procedures vp
            JOIN visits v ON vp.visit_id = v.visit_id
            JOIN patients p ON v.patient_id = p.patient_id
            JOIN cpt_codes c ON vp.cpt_code = c.cpt_code
            ORDER BY vp.procedure_id
            """,
            "Full Billing Summary")

        # Totals
        print_table(cur,
            """
            SELECT
                v.visit_id,
                p.first_name || ' ' || p.last_name AS patient,
                v.visit_date,
                SUM(vp.fee_charged)    AS total_charged,
                SUM(vp.reimbursement)  AS total_reimbursement
            FROM visit_procedures vp
            JOIN visits v ON vp.visit_id = v.visit_id
            JOIN patients p ON v.patient_id = p.patient_id
            GROUP BY v.visit_id, patient, v.visit_date
            """,
            "Visit Totals")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
