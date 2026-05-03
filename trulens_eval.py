"""
TruLens Deep Observability Evaluation — CPT Medical Billing NLP System
=====================================================================
Evaluates the NLP query pipeline across three medical-billing dimensions:
  1. Treatment Revenue  (gross charges → contractual adjustments → net revenue)
  2. Insurance Billing  (CPT-coded claims, reimbursement, EOB/ERA)
  3. Patient Billing    (patient responsibility: copay / deductible / coinsurance)

Metrics produced per test case:
  • Answer Relevance   — does the answer address the question?
  • Groundedness       — is the answer supported by the raw SQL data?
  • Context Relevance  — is the SQL query relevant to the question?
  • Correctness        — does the answer align with medical-billing facts?
"""

import os
import sys
import csv
import json
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

# ── Bridge Replit AI proxy env vars → standard OpenAI env vars ───────────────
# TruLens uses the standard OPENAI_* vars internally.
_proxy_key  = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY", "")
_proxy_base = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL", "")
if _proxy_key:
    os.environ.setdefault("OPENAI_API_KEY",  _proxy_key)
if _proxy_base:
    os.environ.setdefault("OPENAI_BASE_URL", _proxy_base)

# ── Add scripts dir to path for local imports ────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from trulens.core import TruSession, Feedback
from trulens.providers.openai import OpenAI as TruOpenAI

from nlp_query import (
    get_snowflake_connection,
    get_openai_client,
    natural_language_to_sql,
    explain_results,
)
from cortex_agent_workflow import CortexBillingAgent

# ─────────────────────────────────────────────────────────────────────────────
# Medical-billing domain knowledge used as evaluation ground-truth context
# ─────────────────────────────────────────────────────────────────────────────
BILLING_DOMAIN_CONTEXT = """
Medical Billing Revenue Cycle — Ground Truth Reference

1. TREATMENT REVENUE (Service Revenue)
   - fee_charged column = gross charge (what the provider bills for the service)
   - Contractual adjustment = fee_charged minus the insurance-allowed amount
   - Net revenue = total reimbursement collected from all payers
   - The database column reimbursement stores the insurance-allowed/paid amount

2. INSURANCE BILLING (Third-Party Billing)
   - Claims are submitted using CPT/HCPCS procedure codes (cpt_code column)
   - Diagnosis is recorded via ICD-10 codes (diagnosis_code column in visits)
   - Insurance pays the reimbursement amount (reimbursement column)
   - The reimbursement is always <= fee_charged due to contractual discounts
   - Reimbursement rate = reimbursement / fee_charged * 100 percent

3. PATIENT BILLING (Patient Responsibility)
   - Patient responsibility = fee_charged minus reimbursement
   - This represents deductibles, copays, and coinsurance owed by the patient
   - Self-pay patients have no insurance; their entire fee_charged is patient responsibility
   - insurance_id in the patients table identifies insured patients (NULL = self-pay)

4. REVENUE CHAIN
   Treatment rendered → gross charge (fee_charged)
   → claim sent to insurance → insurance pays reimbursement
   → remaining balance billed to patient (fee_charged - reimbursement)
   → net realized revenue = reimbursement + patient_collected_amount

5. DATABASE SCHEMA (Snowflake: cpt_demo.medical)
   patients          : patient_id, first_name, last_name, date_of_birth, age, gender, insurance_id
   cpt_codes         : cpt_code, description, category, base_fee
   visits            : visit_id, patient_id, visit_date, visit_type, provider_name, provider_npi, diagnosis_code, notes
   visit_procedures  : procedure_id, visit_id, cpt_code, units, fee_charged, reimbursement, notes

   KEY FINANCIAL COLUMNS:
     visit_procedures.fee_charged   = gross charge per line item
     visit_procedures.reimbursement = insurance payment per line item
     (fee_charged - reimbursement)  = patient responsibility per line item
"""

# ─────────────────────────────────────────────────────────────────────────────
# Test cases: 12 questions spanning the three billing dimensions
# ─────────────────────────────────────────────────────────────────────────────
TEST_CASES = [
    # ── 1. Treatment Revenue ─────────────────────────────────────────────
    {
        "id": "TR-01",
        "category": "Treatment Revenue",
        "question": "What is the total gross charged amount (fee_charged) across all visit procedures?",
        "expected_concept": "sum of fee_charged = total gross treatment revenue",
    },
    {
        "id": "TR-02",
        "category": "Treatment Revenue",
        "question": "What is the total reimbursement received from insurance for all procedures?",
        "expected_concept": "sum of reimbursement = net insurance revenue",
    },
    {
        "id": "TR-03",
        "category": "Treatment Revenue",
        "question": "What is the contractual adjustment (gross charge minus reimbursement) for each visit?",
        "expected_concept": "fee_charged - reimbursement per visit = contractual write-off",
    },
    {
        "id": "TR-04",
        "category": "Treatment Revenue",
        "question": "Show total gross charges and total reimbursement grouped by CPT code category.",
        "expected_concept": "revenue grouped by service category",
    },
    {
        "id": "TR-05",
        "category": "Treatment Revenue",
        "question": "What is the reimbursement rate percentage (reimbursement divided by fee_charged) for each CPT code?",
        "expected_concept": "reimbursement / fee_charged * 100 = insurance reimbursement rate",
    },
    # ── 2. Insurance Billing ─────────────────────────────────────────────
    {
        "id": "IB-01",
        "category": "Insurance Billing",
        "question": "List all CPT codes billed along with their description, fee charged, and insurance reimbursement amount.",
        "expected_concept": "CPT-coded claim detail with reimbursement",
    },
    {
        "id": "IB-02",
        "category": "Insurance Billing",
        "question": "Which visits had the highest total insurance reimbursement, and what procedures drove it?",
        "expected_concept": "visits ranked by reimbursement with CPT procedure detail",
    },
    {
        "id": "IB-03",
        "category": "Insurance Billing",
        "question": "Show each patient's insurance ID together with their total charges and reimbursements.",
        "expected_concept": "insurance claim summary per patient",
    },
    {
        "id": "IB-04",
        "category": "Insurance Billing",
        "question": "What diagnosis codes (ICD-10) were used and how much was reimbursed for each diagnosis?",
        "expected_concept": "diagnosis code to reimbursement mapping",
    },
    # ── 3. Patient Billing ───────────────────────────────────────────────
    {
        "id": "PB-01",
        "category": "Patient Billing",
        "question": "What is the patient responsibility (fee_charged minus reimbursement) for each visit?",
        "expected_concept": "patient balance due = fee_charged - reimbursement",
    },
    {
        "id": "PB-02",
        "category": "Patient Billing",
        "question": "Which patient has the highest total out-of-pocket balance (total fee_charged minus total reimbursement)?",
        "expected_concept": "patient with largest unpaid balance after insurance",
    },
    {
        "id": "PB-03",
        "category": "Patient Billing",
        "question": "Show a full patient billing statement: patient name, visit date, procedure description, gross charge, insurance paid, and patient balance for each line.",
        "expected_concept": "detailed patient billing statement with EOB breakdown",
    },
    # ── 4. Clinical Query Examples (OpenAI pipeline) ─────────────────────
    {
        "id": "NL-01",
        "category": "Clinical Query Examples",
        "question": "What tables exist in this database?",
        "expected_concept": "lists the four tables: patients, cpt_codes, visits, visit_procedures",
    },
    {
        "id": "NL-02",
        "category": "Clinical Query Examples",
        "question": "Show all patients",
        "expected_concept": "returns all rows from the patients table with patient demographics and insurance ID",
    },
    {
        "id": "NL-03",
        "category": "Clinical Query Examples",
        "question": "What procedures were billed for visit VIS-001?",
        "expected_concept": "returns CPT codes 99395, 90739, 90471 with fee_charged and reimbursement for VIS-001",
    },
    {
        "id": "NL-04",
        "category": "Clinical Query Examples",
        "question": "What is the total charged vs reimbursed per visit?",
        "expected_concept": "VIS-001 total fee_charged=275.00 total reimbursement=220.00 patient_balance=55.00",
    },
    {
        "id": "NL-05",
        "category": "Clinical Query Examples",
        "question": "Which patient had the most expensive visit?",
        "expected_concept": "James Carter PAT-001 VIS-001 total cost 275.00 on 2024-06-10",
    },
    {
        "id": "NL-06",
        "category": "Clinical Query Examples",
        "question": "Show the full billing summary with patient names",
        "expected_concept": "joins patients+visits+visit_procedures+cpt_codes; shows patient name, CPT code, fee_charged, reimbursement, and patient balance per procedure line",
    },
    # ── 5. Cortex Agent — same questions through the new Cortex pipeline ──
    {
        "id": "CA-01",
        "category": "Cortex Agent",
        "question": "Show all patients",
        "expected_concept": "James Carter PAT-001 DOB 1994-03-15 male insurance INS-78432901",
    },
    {
        "id": "CA-02",
        "category": "Cortex Agent",
        "question": "What procedures were billed for visit VIS-001?",
        "expected_concept": "CPT 99395 fee=185 reimb=148; CPT 90739 fee=65 reimb=52; CPT 90471 fee=25 reimb=20",
    },
    {
        "id": "CA-03",
        "category": "Cortex Agent",
        "question": "What is the total charged vs reimbursed per visit?",
        "expected_concept": "VIS-001 total_charged=275 total_reimbursed=220 patient_balance=55",
    },
    {
        "id": "CA-04",
        "category": "Cortex Agent",
        "question": "Which patient had the most expensive visit?",
        "expected_concept": "James Carter VIS-001 total 275.00 on 2024-06-10",
    },
    {
        "id": "CA-05",
        "category": "Cortex Agent",
        "question": "Show the full billing summary with patient names",
        "expected_concept": "patient_name=James Carter all three CPT lines fee_charged reimbursement patient_balance",
    },
    {
        "id": "CA-06",
        "category": "Cortex Agent",
        "question": "Which CPT codes have the highest reimbursement rate?",
        "expected_concept": "CPT 90739 and 90471 both at 80% reimbursement rate; 99395 also 80%",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# Cortex Agent pipeline: Cortex Analyst → SQL → Cortex Complete
# Routes "Cortex Agent" category test cases through the new workflow.
# ─────────────────────────────────────────────────────────────────────────────
def run_cortex_pipeline(question: str) -> dict[str, Any]:
    """Run one question through the CortexBillingAgent pipeline."""
    result: dict[str, Any] = {
        "question":   question,
        "sql":        None,
        "columns":    [],
        "rows":       [],
        "row_count":  0,
        "raw_data":   "",
        "answer":     "",
        "error":      None,
        "latency_ms": 0,
    }
    try:
        agent      = CortexBillingAgent()
        ar         = agent.run(question)
        raw_lines  = [" | ".join(str(v) for v in ar.columns)]
        for row in ar.rows[:20]:
            raw_lines.append(" | ".join(str(v) for v in row))
        result.update({
            "sql":        ar.sql,
            "columns":    ar.columns,
            "rows":       ar.rows,
            "row_count":  ar.row_count,
            "raw_data":   "\n".join(raw_lines) if ar.rows else "(no rows)",
            "answer":     ar.answer,
            "error":      ar.error,
            "latency_ms": ar.total_latency_ms,
        })
    except Exception as exc:
        result["error"]  = str(exc)
        result["answer"] = f"ERROR: {exc}"
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Core NLP pipeline: NL → SQL → execute → explain
# Returns a rich dict for TruLens scoring
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(question: str, conn, ai_client) -> dict[str, Any]:
    """Run the full NLP → SQL → answer pipeline and return all intermediate artifacts."""
    result: dict[str, Any] = {
        "question":    question,
        "sql":         None,
        "columns":     [],
        "rows":        [],
        "row_count":   0,
        "raw_data":    "",
        "answer":      "",
        "error":       None,
        "latency_ms":  0,
    }
    t0 = time.time()
    try:
        sql = natural_language_to_sql(ai_client, question, [])
        result["sql"] = sql

        if sql.startswith("ERROR:"):
            result["error"] = sql
            result["answer"] = sql
            return result

        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            cur.close()

        result["columns"] = cols
        result["rows"]    = [list(r) for r in rows]
        result["row_count"] = len(rows)

        # Build a plain-text data block (used for groundedness scoring)
        lines = ["  ".join(str(c) for c in cols)]
        for row in rows[:20]:          # cap at 20 rows to keep context manageable
            lines.append("  ".join(str(v) for v in row))
        result["raw_data"] = "\n".join(lines) if rows else "(no rows returned)"

        explanation = explain_results(ai_client, question, sql, rows, cols)
        result["answer"] = explanation

    except Exception as exc:
        result["error"]  = str(exc)
        result["answer"] = f"ERROR: {exc}"

    finally:
        result["latency_ms"] = int((time.time() - t0) * 1000)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# TruLens scoring helpers — call feedback functions directly
# ─────────────────────────────────────────────────────────────────────────────
def safe_score(func, *args, **kwargs) -> tuple[float, str]:
    """Call a TruLens feedback function and return (score, reason_string)."""
    try:
        result = func(*args, **kwargs)
        if isinstance(result, tuple) and len(result) == 2:
            score, meta = result
            reason = json.dumps(meta) if isinstance(meta, dict) else str(meta)
            return float(score), reason
        return float(result), ""
    except Exception as exc:
        return -1.0, f"SCORING_ERROR: {exc}"


def score_test_case(case: dict, pipeline_result: dict, provider: TruOpenAI) -> dict:
    """Compute all four TruLens metrics for one test case."""
    question = case["question"]
    answer   = pipeline_result["answer"]
    sql      = pipeline_result["sql"] or ""
    raw_data = pipeline_result["raw_data"]

    # Compose a rich context for groundedness: domain knowledge + raw query data
    grounding_source = (
        f"DOMAIN KNOWLEDGE:\n{BILLING_DOMAIN_CONTEXT}\n\n"
        f"SQL EXECUTED:\n{sql}\n\n"
        f"QUERY RESULTS:\n{raw_data}"
    )

    scores: dict[str, Any] = {}

    # 1. Answer Relevance — does the response answer the question?
    s, r = safe_score(provider.relevance_with_cot_reasons, question, answer)
    scores["answer_relevance"]         = s
    scores["answer_relevance_reasons"] = r

    # 2. Groundedness — is the answer grounded in the retrieved data?
    s, r = safe_score(provider.groundedness_measure_with_cot_reasons, grounding_source, answer)
    scores["groundedness"]         = s
    scores["groundedness_reasons"] = r

    # 3. Context Relevance — is the SQL context relevant to the question?
    sql_context = f"SQL query generated: {sql}\nData returned ({pipeline_result['row_count']} rows): {raw_data}"
    s, r = safe_score(provider.context_relevance_with_cot_reasons, question, sql_context)
    scores["context_relevance"]         = s
    scores["context_relevance_reasons"] = r

    # 4. Correctness — is the answer factually accurate relative to the query results?
    # We embed the actual SQL + raw data as ground-truth evidence so the LLM can
    # judge correctness against retrieved facts rather than hallucinated knowledge.
    correctness_prompt = (
        f"Question asked: {question}\n\n"
        f"Ground-truth evidence (SQL executed and rows returned from Snowflake):\n"
        f"SQL: {sql}\n"
        f"Data ({pipeline_result['row_count']} rows):\n{raw_data}\n\n"
        f"Expected concept: {case['expected_concept']}\n\n"
        f"Evaluate whether the following answer correctly reflects the evidence above.\n"
        f"Answer to evaluate: {answer}"
    )
    s, r = safe_score(provider.correctness_with_cot_reasons, correctness_prompt, answer)
    scores["correctness"]         = s
    scores["correctness_reasons"] = r

    return scores


# ─────────────────────────────────────────────────────────────────────────────
# Report formatters
# ─────────────────────────────────────────────────────────────────────────────
SCORE_THRESHOLDS = {"green": 0.75, "yellow": 0.50}

def label(score: float) -> str:
    if score < 0:
        return "ERROR"
    if score >= SCORE_THRESHOLDS["green"]:
        return "PASS"
    if score >= SCORE_THRESHOLDS["yellow"]:
        return "WARN"
    return "FAIL"

def bar(score: float, width: int = 20) -> str:
    if score < 0:
        return "[ERROR         ]"
    filled = int(score * width)
    return "[" + "█" * filled + "░" * (width - filled) + f"] {score:.2f}"


def print_report(results: list[dict]) -> None:
    print()
    print("═" * 90)
    print("  TruLens Deep Observability — CPT Medical Billing NLP Evaluation Report")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 90)

    category_scores: dict[str, list] = {}
    metric_keys = ["answer_relevance", "groundedness", "context_relevance", "correctness"]

    for r in results:
        cat = r["category"]
        if cat not in category_scores:
            category_scores[cat] = []

        print()
        print(f"  ┌─ [{r['id']}] {cat}")
        print(f"  │  Q: {r['question'][:80]}")
        print(f"  │  Expected concept: {r['expected_concept'][:70]}")
        if r.get("error"):
            print(f"  │  ⚠ Pipeline error: {r['error']}")
        else:
            print(f"  │  SQL: {(r.get('sql') or '')[:80]}")
            print(f"  │  Rows: {r['row_count']}  |  Latency: {r['latency_ms']}ms")
            print(f"  │  Answer: {r['answer'][:120]}")
        print(f"  │")
        print(f"  │  Scores:")
        case_scores = []
        for k in metric_keys:
            s = r.get(k, -1.0)
            lbl = label(s)
            print(f"  │    {k:<22} {bar(s)}  [{lbl}]")
            if s >= 0:
                case_scores.append(s)
        avg = sum(case_scores) / len(case_scores) if case_scores else -1.0
        category_scores[cat].append(avg)
        print(f"  │    {'AVERAGE':<22} {bar(avg)}")
        print(f"  └{'─' * 60}")

    # ── Summary by category ──────────────────────────────────────────────────
    print()
    print("─" * 90)
    print("  SUMMARY BY BILLING CATEGORY")
    print("─" * 90)
    all_avgs = []
    for cat, avgs in category_scores.items():
        valid = [a for a in avgs if a >= 0]
        cat_avg = sum(valid) / len(valid) if valid else -1.0
        all_avgs.extend(valid)
        print(f"  {cat:<32} {bar(cat_avg, 30)}  n={len(avgs)}")

    overall = sum(all_avgs) / len(all_avgs) if all_avgs else -1.0
    print()
    print(f"  {'OVERALL PIPELINE QUALITY':<32} {bar(overall, 30)}")
    print("═" * 90)
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Main evaluation runner
# ─────────────────────────────────────────────────────────────────────────────
def run_evaluation(
    test_cases: list[dict] | None = None,
    output_csv: str | None = None,
    output_json: str | None = None,
) -> list[dict]:
    """
    Run the full TruLens evaluation suite and return a list of result records.

    Parameters
    ----------
    test_cases  : subset of TEST_CASES to run (default: all)
    output_csv  : if set, write results to this CSV path
    output_json : if set, write full results to this JSON path
    """
    cases    = test_cases or TEST_CASES
    run_ts   = datetime.now().isoformat()

    print(f"\n  Initializing TruLens session …")
    session = TruSession()

    print(f"  Connecting to Snowflake …")
    conn = get_snowflake_connection()

    print(f"  Loading OpenAI client …")
    ai_client = get_openai_client()

    print(f"  Initializing TruLens OpenAI provider for scoring …")
    provider = TruOpenAI(model_engine="gpt-4.1-mini")

    print(f"\n  Running {len(cases)} test cases …\n")

    all_results: list[dict] = []

    for i, case in enumerate(cases, 1):
        print(f"  [{i:02d}/{len(cases)}] {case['id']} — {case['question'][:60]} …", end="", flush=True)

        # Route Cortex Agent cases through the new pipeline
        if case["category"] == "Cortex Agent":
            pipeline = run_cortex_pipeline(case["question"])
        else:
            pipeline = run_pipeline(case["question"], conn, ai_client)
        scores   = score_test_case(case, pipeline, provider)

        record = {
            "run_ts":          run_ts,
            "id":              case["id"],
            "category":        case["category"],
            "question":        case["question"],
            "expected_concept": case["expected_concept"],
            "sql":             pipeline["sql"],
            "row_count":       pipeline["row_count"],
            "latency_ms":      pipeline["latency_ms"],
            "answer":          pipeline["answer"],
            "error":           pipeline["error"],
            **scores,
        }
        all_results.append(record)

        avg_score = sum(
            record.get(k, 0) for k in
            ["answer_relevance", "groundedness", "context_relevance", "correctness"]
            if record.get(k, -1) >= 0
        )
        valid_n = sum(
            1 for k in ["answer_relevance", "groundedness", "context_relevance", "correctness"]
            if record.get(k, -1) >= 0
        )
        avg = avg_score / valid_n if valid_n else -1
        print(f" avg={avg:.2f}")

    conn.close()

    # ── Persist results ───────────────────────────────────────────────────
    scripts_dir = Path(__file__).parent

    if output_csv is None:
        output_csv = str(scripts_dir / f"eval_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    if output_json is None:
        output_json = str(scripts_dir / f"eval_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

    # CSV
    metric_keys = ["answer_relevance", "groundedness", "context_relevance", "correctness"]
    csv_fields  = [
        "run_ts", "id", "category", "question", "expected_concept",
        "sql", "row_count", "latency_ms", "answer", "error",
        *metric_keys,
        *[f"{k}_reasons" for k in metric_keys],
    ]
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\n  CSV  → {output_csv}")

    # JSON
    with open(output_json, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"  JSON → {output_json}")

    # ── Print human-readable report ───────────────────────────────────────
    print_report(all_results)

    return all_results


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="TruLens evaluation for CPT Medical Billing NLP"
    )
    ap.add_argument(
        "--category",
        choices=[
            "Treatment Revenue",
            "Insurance Billing",
            "Patient Billing",
            "Clinical Query Examples",
            "Cortex Agent",
        ],
        help="Run only test cases in this billing category",
    )
    ap.add_argument("--csv",  dest="csv_path",  default=None, help="CSV output path")
    ap.add_argument("--json", dest="json_path", default=None, help="JSON output path")
    ap.add_argument(
        "--ids",
        nargs="+",
        help="Run only specific test case IDs e.g. TR-01 IB-02",
    )
    args = ap.parse_args()

    cases = TEST_CASES
    if args.category:
        cases = [c for c in cases if c["category"] == args.category]
    if args.ids:
        cases = [c for c in cases if c["id"] in args.ids]

    try:
        run_evaluation(test_cases=cases, output_csv=args.csv_path, output_json=args.json_path)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
