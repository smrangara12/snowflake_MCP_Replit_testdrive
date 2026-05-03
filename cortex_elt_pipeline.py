#!/usr/bin/env python3
"""
Snowflake Cortex ELT Pipeline — CPT Medical Billing Insights
=============================================================
Code-first ELT: creates 6 aggregate tables + 1 Cortex AI insights
table, all materialized as Snowflake tables (not views).

Aggregate tables (refreshed on every run):
  AGG_PATIENT_BILLING    — per-patient financial summary
  AGG_CPT_REVENUE        — CPT code performance & utilization
  AGG_DISEASE_BILLING    — billing grouped by diagnosis/disease
  AGG_MONTHLY_REVENUE    — month-over-month revenue trends
  AGG_PAYER_MIX          — insured vs self-pay breakdown
  AGG_PROVIDER_METRICS   — per-provider productivity
  CORTEX_INSIGHTS        — Cortex-generated AI narrative insights

Run:  python cortex_elt_pipeline.py
"""
import os, sys, json, time
from datetime import datetime
import snowflake.connector

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"].replace(".snowflakecomputing.com", ""),
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        warehouse="COMPUTE_WH",
        database="cpt_demo",
        schema="medical",
    )

# ─────────────────────────────────────────────────────────────────────────────
# ELT stage definitions — each entry: (table_name, CTAS_sql)
# ─────────────────────────────────────────────────────────────────────────────
ELT_STAGES = [

    ("AGG_PATIENT_BILLING", """
CREATE OR REPLACE TABLE cpt_demo.medical.agg_patient_billing AS
SELECT
    p.patient_id,
    p.first_name || ' ' || p.last_name          AS patient_name,
    p.gender,
    p.age,
    CASE WHEN p.insurance_id IS NULL
         THEN 'Self-Pay' ELSE 'Insured' END      AS payer_type,
    p.insurance_id,
    COUNT(DISTINCT v.visit_id)                   AS visit_count,
    COUNT(vp.procedure_id)                       AS procedure_count,
    SUM(vp.fee_charged)                          AS total_charged,
    SUM(vp.reimbursement)                        AS total_reimbursed,
    SUM(vp.fee_charged - vp.reimbursement)       AS patient_balance,
    ROUND(SUM(vp.reimbursement) /
          NULLIF(SUM(vp.fee_charged),0) * 100, 1) AS reimbursement_rate_pct,
    MIN(v.visit_date)                            AS first_visit_date,
    MAX(v.visit_date)                            AS last_visit_date
FROM cpt_demo.medical.patients p
JOIN cpt_demo.medical.visits v ON p.patient_id = v.patient_id
JOIN cpt_demo.medical.visit_procedures vp ON v.visit_id = vp.visit_id
GROUP BY p.patient_id, p.first_name, p.last_name, p.gender, p.age,
         p.insurance_id
ORDER BY total_charged DESC
"""),

    ("AGG_CPT_REVENUE", """
CREATE OR REPLACE TABLE cpt_demo.medical.agg_cpt_revenue AS
SELECT
    c.cpt_code,
    c.description,
    c.category,
    c.base_fee,
    COUNT(vp.procedure_id)                        AS utilization_count,
    SUM(vp.units)                                 AS total_units,
    SUM(vp.fee_charged)                           AS total_charged,
    SUM(vp.reimbursement)                         AS total_reimbursed,
    SUM(vp.fee_charged - vp.reimbursement)        AS total_patient_balance,
    ROUND(AVG(vp.fee_charged), 2)                 AS avg_charge_per_unit,
    ROUND(AVG(vp.reimbursement), 2)               AS avg_reimbursement_per_unit,
    ROUND(SUM(vp.reimbursement) /
          NULLIF(SUM(vp.fee_charged),0) * 100, 1) AS reimbursement_rate_pct
FROM cpt_demo.medical.cpt_codes c
JOIN cpt_demo.medical.visit_procedures vp ON c.cpt_code = vp.cpt_code
GROUP BY c.cpt_code, c.description, c.category, c.base_fee
ORDER BY total_charged DESC
"""),

    ("AGG_DISEASE_BILLING", """
CREATE OR REPLACE TABLE cpt_demo.medical.agg_disease_billing AS
SELECT
    v.diagnosis_code,
    CASE v.diagnosis_code
        WHEN 'Z00.00'  THEN 'General Adult Medical Exam (Preventive)'
        WHEN 'E11.9'   THEN 'Type 2 Diabetes Mellitus'
        WHEN 'E11.65'  THEN 'Type 2 Diabetes with Hyperglycemia'
        WHEN 'I10'     THEN 'Essential Hypertension'
        WHEN 'I25.10'  THEN 'Coronary Artery Disease'
        WHEN 'J18.9'   THEN 'Pneumonia'
        WHEN 'M54.50'  THEN 'Low Back Pain'
        WHEN 'K21.0'   THEN 'GERD with Esophagitis'
        WHEN 'F32.1'   THEN 'Major Depressive Disorder'
        WHEN 'J45.909' THEN 'Mild Intermittent Asthma'
        WHEN 'E78.5'   THEN 'Hyperlipidemia'
        WHEN 'N39.0'   THEN 'Urinary Tract Infection'
        WHEN 'Z23'     THEN 'Vaccine / Immunization Encounter'
        ELSE v.diagnosis_code
    END                                           AS disease_label,
    COUNT(DISTINCT v.visit_id)                    AS visit_count,
    COUNT(DISTINCT v.patient_id)                  AS patient_count,
    SUM(vp.fee_charged)                           AS total_charged,
    ROUND(AVG(vp.fee_charged), 2)                 AS avg_charge_per_procedure,
    ROUND(SUM(vp.fee_charged) /
          COUNT(DISTINCT v.visit_id), 2)          AS avg_charge_per_visit,
    SUM(vp.reimbursement)                         AS total_reimbursed,
    SUM(vp.fee_charged - vp.reimbursement)        AS total_patient_balance,
    ROUND(SUM(vp.reimbursement) /
          NULLIF(SUM(vp.fee_charged),0) * 100, 1) AS reimbursement_rate_pct
FROM cpt_demo.medical.visits v
JOIN cpt_demo.medical.visit_procedures vp ON v.visit_id = vp.visit_id
GROUP BY v.diagnosis_code
ORDER BY total_charged DESC
"""),

    ("AGG_MONTHLY_REVENUE", """
CREATE OR REPLACE TABLE cpt_demo.medical.agg_monthly_revenue AS
SELECT
    DATE_TRUNC('MONTH', v.visit_date)             AS revenue_month,
    TO_CHAR(DATE_TRUNC('MONTH', v.visit_date),
            'Mon YYYY')                           AS month_label,
    COUNT(DISTINCT v.visit_id)                    AS visit_count,
    COUNT(DISTINCT v.patient_id)                  AS active_patients,
    COUNT(vp.procedure_id)                        AS procedure_count,
    SUM(vp.fee_charged)                           AS total_charged,
    SUM(vp.reimbursement)                         AS total_reimbursed,
    SUM(vp.fee_charged - vp.reimbursement)        AS patient_balance,
    ROUND(SUM(vp.reimbursement) /
          NULLIF(SUM(vp.fee_charged),0) * 100, 1) AS reimbursement_rate_pct
FROM cpt_demo.medical.visits v
JOIN cpt_demo.medical.visit_procedures vp ON v.visit_id = vp.visit_id
GROUP BY DATE_TRUNC('MONTH', v.visit_date)
ORDER BY revenue_month
"""),

    ("AGG_PAYER_MIX", """
CREATE OR REPLACE TABLE cpt_demo.medical.agg_payer_mix AS
SELECT
    CASE WHEN p.insurance_id IS NULL
         THEN 'Self-Pay' ELSE 'Insured' END       AS payer_type,
    COUNT(DISTINCT p.patient_id)                  AS patient_count,
    COUNT(DISTINCT v.visit_id)                    AS visit_count,
    COUNT(vp.procedure_id)                        AS procedure_count,
    SUM(vp.fee_charged)                           AS total_charged,
    SUM(vp.reimbursement)                         AS total_reimbursed,
    SUM(vp.fee_charged - vp.reimbursement)        AS patient_balance,
    ROUND(SUM(vp.reimbursement) /
          NULLIF(SUM(vp.fee_charged),0) * 100, 1) AS reimbursement_rate_pct,
    ROUND(SUM(vp.fee_charged) /
          COUNT(DISTINCT v.visit_id), 2)          AS avg_revenue_per_visit
FROM cpt_demo.medical.patients p
JOIN cpt_demo.medical.visits v ON p.patient_id = v.patient_id
JOIN cpt_demo.medical.visit_procedures vp ON v.visit_id = vp.visit_id
GROUP BY CASE WHEN p.insurance_id IS NULL THEN 'Self-Pay' ELSE 'Insured' END
ORDER BY payer_type
"""),

    ("AGG_PROVIDER_METRICS", """
CREATE OR REPLACE TABLE cpt_demo.medical.agg_provider_metrics AS
SELECT
    v.provider_name,
    v.provider_npi,
    COUNT(DISTINCT v.visit_id)                    AS visit_count,
    COUNT(DISTINCT v.patient_id)                  AS patient_count,
    COUNT(vp.procedure_id)                        AS procedure_count,
    SUM(vp.fee_charged)                           AS total_charged,
    SUM(vp.reimbursement)                         AS total_reimbursed,
    SUM(vp.fee_charged - vp.reimbursement)        AS patient_balance,
    ROUND(SUM(vp.fee_charged) /
          COUNT(DISTINCT v.visit_id), 2)          AS avg_revenue_per_visit,
    ROUND(SUM(vp.reimbursement) /
          NULLIF(SUM(vp.fee_charged),0) * 100, 1) AS reimbursement_rate_pct
FROM cpt_demo.medical.visits v
JOIN cpt_demo.medical.visit_procedures vp ON v.visit_id = vp.visit_id
GROUP BY v.provider_name, v.provider_npi
ORDER BY total_charged DESC
"""),
]

# ─────────────────────────────────────────────────────────────────────────────
# Cortex Insights: query each agg table, then call CORTEX.COMPLETE()
# ─────────────────────────────────────────────────────────────────────────────
INSIGHT_DEFINITIONS = [
    {
        "insight_id":   "INS-001",
        "insight_type": "Revenue Cycle",
        "title":        "Overall Revenue Cycle Health",
        "sql": """
            SELECT
                COUNT(DISTINCT visit_id)   AS total_visits,
                COUNT(DISTINCT patient_id) AS total_patients,
                SUM(fee_charged)           AS gross_charges,
                SUM(reimbursement)         AS total_reimbursed,
                SUM(patient_balance)       AS total_patient_balance,
                ROUND(SUM(reimbursement)/NULLIF(SUM(fee_charged),0)*100,1) AS reimbursement_rate
            FROM cpt_demo.medical.billing_summary
        """,
        "prompt": (
            "You are a medical billing analyst. Here is the revenue cycle summary:\n{data}\n\n"
            "Write a 3-sentence executive summary covering: (1) total gross charges and "
            "reimbursement rate, (2) patient balance exposure, (3) one key observation or "
            "recommendation. Be specific with dollar amounts."
        ),
    },
    {
        "insight_id":   "INS-002",
        "insight_type": "Disease Burden",
        "title":        "Top Disease Categories by Revenue",
        "sql": """
            SELECT disease_label, visit_count, total_charged,
                   reimbursement_rate_pct
            FROM cpt_demo.medical.agg_disease_billing
            ORDER BY total_charged DESC LIMIT 5
        """,
        "prompt": (
            "You are a medical billing analyst. Here are the top 5 diagnoses by revenue:\n{data}\n\n"
            "Write a 3-sentence clinical revenue insight covering: (1) which diseases drive the "
            "most billing activity, (2) reimbursement patterns across conditions, (3) a strategic "
            "observation about payer mix or care management opportunity."
        ),
    },
    {
        "insight_id":   "INS-003",
        "insight_type": "Payer Mix",
        "title":        "Insured vs Self-Pay Analysis",
        "sql": """
            SELECT payer_type, patient_count, visit_count,
                   total_charged, total_reimbursed, patient_balance,
                   reimbursement_rate_pct, avg_revenue_per_visit
            FROM cpt_demo.medical.agg_payer_mix
        """,
        "prompt": (
            "You are a medical billing analyst. Here is the payer mix breakdown:\n{data}\n\n"
            "Write a 3-sentence payer mix analysis covering: (1) revenue split between insured "
            "and self-pay patients, (2) collection risk from self-pay patient_balance, "
            "(3) a recommendation to improve collection rates or reduce bad debt exposure."
        ),
    },
    {
        "insight_id":   "INS-004",
        "insight_type": "CPT Performance",
        "title":        "Highest Revenue CPT Codes",
        "sql": """
            SELECT cpt_code, description, category, utilization_count,
                   total_charged, reimbursement_rate_pct
            FROM cpt_demo.medical.agg_cpt_revenue
            ORDER BY total_charged DESC LIMIT 6
        """,
        "prompt": (
            "You are a medical billing analyst. Here are the top 6 CPT codes by revenue:\n{data}\n\n"
            "Write a 3-sentence CPT performance insight covering: (1) which procedure categories "
            "generate the most revenue, (2) any notable differences in reimbursement rates "
            "across service types, (3) a utilization or coding efficiency observation."
        ),
    },
    {
        "insight_id":   "INS-005",
        "insight_type": "Monthly Trend",
        "title":        "Monthly Revenue Trend",
        "sql": """
            SELECT month_label, visit_count, total_charged,
                   total_reimbursed, reimbursement_rate_pct
            FROM cpt_demo.medical.agg_monthly_revenue
            ORDER BY revenue_month
        """,
        "prompt": (
            "You are a medical billing analyst. Here is the month-over-month revenue trend:\n{data}\n\n"
            "Write a 3-sentence trend analysis covering: (1) overall revenue trajectory, "
            "(2) the month with highest and lowest billing activity, (3) a seasonal or "
            "operational pattern observation with a forward-looking recommendation."
        ),
    },
    {
        "insight_id":   "INS-006",
        "insight_type": "Provider Performance",
        "title":        "Provider Productivity & Revenue",
        "sql": """
            SELECT provider_name, visit_count, patient_count,
                   total_charged, reimbursement_rate_pct,
                   avg_revenue_per_visit
            FROM cpt_demo.medical.agg_provider_metrics
            ORDER BY total_charged DESC
        """,
        "prompt": (
            "You are a medical billing analyst. Here is provider productivity data:\n{data}\n\n"
            "Write a 3-sentence provider analysis covering: (1) which provider generates the "
            "most revenue and why, (2) differences in average revenue per visit across providers, "
            "(3) a recommendation for scheduling, coding quality, or workload optimization."
        ),
    },
]


def _rows_to_text(columns, rows):
    """Convert SQL result to plain text for the LLM."""
    lines = [" | ".join(str(c) for c in columns)]
    for row in rows:
        lines.append(" | ".join(str(v) for v in row))
    return "\n".join(lines)


def _cortex_complete(conn, prompt: str) -> str:
    """Call SNOWFLAKE.CORTEX.COMPLETE() via SQL."""
    escaped = prompt.replace("'", "\\'").replace("\\", "\\\\")
    sql = f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'mistral-large2',
  ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT('role','system','content',
      'You are a senior medical billing analyst. Write clear, data-driven insights.'),
    OBJECT_CONSTRUCT('role','user','content','{escaped}')
  ),
  OBJECT_CONSTRUCT('temperature', 0, 'max_tokens', 400)
) AS insight
"""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        raw = cur.fetchone()[0] or ""
    finally:
        cur.close()

    # Unwrap JSON envelope if returned
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            choices = parsed.get("choices", [])
            if choices:
                raw = (choices[0].get("messages", "")
                       or choices[0].get("message", {}).get("content", "")
                       or raw)
    except (json.JSONDecodeError, TypeError):
        pass
    return raw.strip()


def run_elt(conn):
    """Execute all ELT stages and generate Cortex insights."""
    cur = conn.cursor()
    generated_at = datetime.utcnow().isoformat()

    # ── Stage 1-6: Build aggregate tables ─────────────────────────────────────
    print("\n  ── ELT Stages ──────────────────────────────────────────────────")
    for table_name, sql in ELT_STAGES:
        t0 = time.time()
        cur.execute(sql)
        cur.execute(f"SELECT COUNT(*) FROM cpt_demo.medical.{table_name.lower()}")
        n = cur.fetchone()[0]
        elapsed = int((time.time() - t0) * 1000)
        print(f"  ✓ {table_name:<30} {n:>4} rows  {elapsed}ms")

    # ── Stage 7: Cortex AI Narrative Insights ────────────────────────────────
    print("\n  ── Cortex AI Insights ──────────────────────────────────────────")
    insights_rows = []
    for defn in INSIGHT_DEFINITIONS:
        t0 = time.time()
        cur.execute(defn["sql"])
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        data_text = _rows_to_text(cols, rows)
        prompt = defn["prompt"].format(data=data_text)
        insight_text = _cortex_complete(conn, prompt)
        elapsed = int((time.time() - t0) * 1000)
        print(f"  ✓ {defn['insight_id']} {defn['title'][:40]:<40} {elapsed}ms")
        insights_rows.append((
            defn["insight_id"],
            defn["insight_type"],
            defn["title"],
            insight_text,
            data_text,
            generated_at,
        ))

    # ── Write CORTEX_INSIGHTS table ───────────────────────────────────────────
    cur.execute("""
      CREATE OR REPLACE TABLE cpt_demo.medical.cortex_insights (
        insight_id    VARCHAR(10)   NOT NULL,
        insight_type  VARCHAR(50),
        title         VARCHAR(100),
        summary       TEXT,
        data_context  TEXT,
        generated_at  VARCHAR(30)
      )
    """)
    cur.executemany(
        "INSERT INTO cpt_demo.medical.cortex_insights VALUES (%s,%s,%s,%s,%s,%s)",
        insights_rows,
    )
    print(f"\n  ✓ CORTEX_INSIGHTS              {len(insights_rows):>4} rows  (AI-generated)")
    cur.close()


def get_all_insights(conn) -> dict:
    """
    Query all aggregate tables and return as a structured dict.
    Used by the Express API to serve the Insights UI.
    """
    cur = conn.cursor()

    def fetchall(sql):
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    result = {
        "generated_at": datetime.utcnow().isoformat(),
        "kpi": fetchall("""
            SELECT
                COUNT(DISTINCT patient_id)   AS total_patients,
                COUNT(DISTINCT visit_id)     AS total_visits,
                COUNT(procedure_id)          AS total_procedures,
                ROUND(SUM(fee_charged),2)    AS gross_charges,
                ROUND(SUM(reimbursement),2)  AS total_reimbursed,
                ROUND(SUM(patient_balance),2)AS total_patient_balance,
                ROUND(SUM(reimbursement)/NULLIF(SUM(fee_charged),0)*100,1)
                                             AS overall_reimbursement_rate
            FROM cpt_demo.medical.billing_summary
        """),
        "patient_billing":   fetchall("SELECT * FROM cpt_demo.medical.agg_patient_billing"),
        "cpt_revenue":       fetchall("SELECT * FROM cpt_demo.medical.agg_cpt_revenue"),
        "disease_billing":   fetchall("SELECT * FROM cpt_demo.medical.agg_disease_billing"),
        "monthly_revenue":   fetchall("SELECT * FROM cpt_demo.medical.agg_monthly_revenue"),
        "payer_mix":         fetchall("SELECT * FROM cpt_demo.medical.agg_payer_mix"),
        "provider_metrics":  fetchall("SELECT * FROM cpt_demo.medical.agg_provider_metrics"),
        "cortex_insights":   fetchall("SELECT * FROM cpt_demo.medical.cortex_insights"),
    }

    cur.close()
    return result


def _serialize(obj):
    """JSON-serializable coercion for Decimal/date types."""
    import decimal, datetime
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return str(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--json",       action="store_true", help="Print insights JSON to stdout")
    parser.add_argument("--read-cache", action="store_true", help="Alias for --json")
    args = parser.parse_args()

    conn = get_conn()
    try:
        if args.json or args.read_cache:
            # API mode: just read and dump JSON
            result = get_all_insights(conn)
            print(json.dumps(result, default=_serialize))
        else:
            # CLI mode: run full ELT then summarize
            print("\n  Snowflake Cortex ELT Pipeline — CPT Medical Billing\n")
            run_elt(conn)
            result = get_all_insights(conn)
            kpi = result["kpi"][0]
            print(f"""
  ── Summary ─────────────────────────────────────────────────────
    Patients:             {kpi['total_patients']}
    Visits:               {kpi['total_visits']}
    Gross Charges:        ${float(kpi['gross_charges']):,.2f}
    Total Reimbursed:     ${float(kpi['total_reimbursed']):,.2f}
    Patient Balance:      ${float(kpi['total_patient_balance']):,.2f}
    Reimbursement Rate:   {kpi['overall_reimbursement_rate']}%
  ────────────────────────────────────────────────────────────────
""")
    finally:
        conn.close()
