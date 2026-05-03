#!/usr/bin/env python3
"""
Snowflake Cortex Code-First Agentic Workflow Orchestration
==========================================================
CPT Medical Billing NLP System — v2

Architecture (defined entirely in Python code — no UI config):

    ┌──────────────────────────────────────────────────────────────┐
    │                    CortexBillingAgent                        │
    │                                                              │
    │  TOOL 1               TOOL 2               TOOL 3           │
    │  ┌────────────┐      ┌────────────┐      ┌─────────────┐   │
    │  │ Cortex     │      │ Snowflake  │      │ Cortex      │   │
    │  │ Analyst    │ ───► │ SQL        │ ───► │ Complete    │   │
    │  │ REST API   │      │ Executor   │      │ (via SQL)   │   │
    │  │ NL → SQL   │      │ run rows   │      │ explain     │   │
    │  └────────────┘      └────────────┘      └─────────────┘   │
    │   semantic model          connector          mistral-large2  │
    │   defined inline          (PAT auth)         answer gen      │
    └──────────────────────────────────────────────────────────────┘

All three tools are declared in Python code.
Semantic model is defined inline as a YAML constant — no external
files, no Snowflake UI, no drag-and-drop configuration.
"""

import os
import sys
import json
import time
import textwrap
import re
import requests
import snowflake.connector
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# TOOL 1 — Semantic model declared inline (code-first)
# Cortex Analyst converts natural-language billing questions to Snowflake SQL.
# ─────────────────────────────────────────────────────────────────────────────
SEMANTIC_MODEL_YAML = """
name: cpt_medical_billing
description: >
  CPT medical billing — four tables joined into one flat billing_summary view.
  fee_charged = gross charge billed;
  reimbursement = insurance payment;
  patient_balance = fee_charged minus reimbursement (copay + deductible).

tables:

  # ── Primary table: flat view joining all four source tables ─────────────
  # All billing questions route through billing_summary for simplicity.
  - name: billing_summary
    description: >
      Flat billing view joining visit_procedures, visits, patients, cpt_codes.
      One row per procedure line per visit. Use this for any question that
      involves patient names, visit dates, CPT descriptions, or financial totals.
    base_table:
      database: cpt_demo
      schema: medical
      table: billing_summary
    measures:
      - name: fee_charged
        description: Gross charge amount billed per procedure line
        expr: fee_charged
        data_type: number
        default_aggregation: sum
        synonyms:
          - gross charge
          - billed amount
          - treatment revenue
          - charged amount
          - total charged
      - name: reimbursement
        description: Insurance payment / allowed amount per procedure line
        expr: reimbursement
        data_type: number
        default_aggregation: sum
        synonyms:
          - insurance payment
          - insurance reimbursement
          - allowed amount
          - amount paid by insurance
      - name: patient_balance
        description: >
          Patient out-of-pocket responsibility
          (deductible + copay = fee_charged minus reimbursement)
        expr: patient_balance
        data_type: number
        default_aggregation: sum
        synonyms:
          - patient responsibility
          - patient balance
          - out of pocket
          - copay
          - deductible
          - amount owed by patient
      - name: units
        description: Number of procedure units billed
        expr: units
        data_type: number
        default_aggregation: sum
      - name: base_fee
        description: Standard CPT code base fee
        expr: base_fee
        data_type: number
        default_aggregation: avg
        synonyms:
          - standard fee
          - list price
    dimensions:
      - name: procedure_id
        description: Unique procedure record identifier
        expr: procedure_id
        data_type: text
      - name: visit_id
        description: Visit identifier
        expr: visit_id
        data_type: text
        sample_values: ["VIS-001"]
      - name: visit_date
        description: Date the visit occurred
        expr: visit_date
        data_type: date
      - name: visit_type
        description: Type of visit (Preventive, Sick, Follow-up, etc.)
        expr: visit_type
        data_type: text
      - name: provider_name
        description: Name of the treating provider / physician
        expr: provider_name
        data_type: text
        synonyms:
          - doctor
          - physician
          - provider
      - name: diagnosis_code
        description: ICD-10 diagnosis code
        expr: diagnosis_code
        data_type: text
      - name: cpt_code
        description: CPT/HCPCS procedure code
        expr: cpt_code
        data_type: text
        sample_values: ["99395", "90739", "90471"]
      - name: procedure_description
        description: Human-readable procedure name
        expr: procedure_description
        data_type: text
        synonyms:
          - procedure name
          - service name
          - procedure
      - name: procedure_category
        description: >
          Procedure category: Evaluation & Management, Vaccine, Administration, etc.
        expr: procedure_category
        data_type: text
        synonyms:
          - procedure type
          - service category
      - name: patient_id
        description: Unique patient identifier
        expr: patient_id
        data_type: text
        sample_values: ["PAT-001"]
      - name: patient_name
        description: Patient full name (first + last)
        expr: patient_name
        data_type: text
        synonyms:
          - patient
          - name
          - full name
      - name: first_name
        description: Patient first name
        expr: first_name
        data_type: text
      - name: last_name
        description: Patient last name
        expr: last_name
        data_type: text
      - name: gender
        description: Patient gender
        expr: gender
        data_type: text
      - name: insurance_id
        description: >
          Insurance plan identifier. NULL = self-pay.
        expr: insurance_id
        data_type: text
        synonyms:
          - insurance
          - payer

  # ── Individual tables for schema-level or single-table queries ──────────
  - name: patients
    description: Patient demographics and insurance information.
    base_table:
      database: cpt_demo
      schema: medical
      table: patients
    primary_key:
      columns: [patient_id]
    measures:
      - name: age
        description: Patient age in years
        expr: age
        data_type: number
        default_aggregation: avg
    dimensions:
      - name: patient_id
        description: Unique patient identifier
        expr: patient_id
        data_type: text
      - name: first_name
        expr: first_name
        data_type: text
      - name: last_name
        expr: last_name
        data_type: text
      - name: date_of_birth
        expr: date_of_birth
        data_type: date
      - name: gender
        expr: gender
        data_type: text
      - name: insurance_id
        description: Insurance plan identifier (NULL = self-pay)
        expr: insurance_id
        data_type: text

  - name: visits
    description: Patient visit records (header, one per visit).
    base_table:
      database: cpt_demo
      schema: medical
      table: visits
    primary_key:
      columns: [visit_id]
    dimensions:
      - name: visit_id
        expr: visit_id
        data_type: text
      - name: patient_id
        expr: patient_id
        data_type: text
      - name: visit_date
        expr: visit_date
        data_type: date
      - name: visit_type
        expr: visit_type
        data_type: text
      - name: provider_name
        expr: provider_name
        data_type: text
      - name: diagnosis_code
        expr: diagnosis_code
        data_type: text

  - name: visit_procedures
    description: Line-item billing records per CPT code per visit.
    base_table:
      database: cpt_demo
      schema: medical
      table: visit_procedures
    primary_key:
      columns: [procedure_id]
    measures:
      - name: fee_charged
        expr: fee_charged
        data_type: number
        default_aggregation: sum
      - name: reimbursement
        expr: reimbursement
        data_type: number
        default_aggregation: sum
      - name: patient_balance
        expr: fee_charged - reimbursement
        data_type: number
        default_aggregation: sum
      - name: units
        expr: units
        data_type: number
        default_aggregation: sum
    dimensions:
      - name: procedure_id
        expr: procedure_id
        data_type: text
      - name: visit_id
        expr: visit_id
        data_type: text
      - name: cpt_code
        expr: cpt_code
        data_type: text

  - name: cpt_codes
    description: CPT/HCPCS procedure code reference with base fees.
    base_table:
      database: cpt_demo
      schema: medical
      table: cpt_codes
    primary_key:
      columns: [cpt_code]
    measures:
      - name: base_fee
        expr: base_fee
        data_type: number
        default_aggregation: avg
    dimensions:
      - name: cpt_code
        expr: cpt_code
        data_type: text
      - name: description
        expr: description
        data_type: text
      - name: category
        expr: category
        data_type: text

  # ── Aggregate tables (ELT pipeline outputs) ──────────────────────────────
  - name: agg_patient_billing
    description: >
      Pre-aggregated patient-level financial summary. One row per patient.
      Use for patient-level revenue analysis, reimbursement rates, or balance comparisons.
    base_table:
      database: cpt_demo
      schema: medical
      table: agg_patient_billing
    measures:
      - name: total_charged
        description: Total gross charges for this patient
        expr: total_charged
        data_type: number
        default_aggregation: sum
        synonyms: [gross charges, billed total, total billed]
      - name: total_reimbursed
        description: Total insurance reimbursement received
        expr: total_reimbursed
        data_type: number
        default_aggregation: sum
        synonyms: [insurance payment, amount reimbursed]
      - name: patient_balance
        description: Outstanding patient responsibility (charged minus reimbursed)
        expr: patient_balance
        data_type: number
        default_aggregation: sum
        synonyms: [balance due, patient owes, out of pocket]
      - name: reimbursement_rate_pct
        description: Percentage of charges reimbursed by insurance
        expr: reimbursement_rate_pct
        data_type: number
        default_aggregation: avg
        synonyms: [collection rate, reimbursement rate, payment rate]
      - name: visit_count
        description: Total number of visits for this patient
        expr: visit_count
        data_type: number
        default_aggregation: sum
    dimensions:
      - name: patient_id
        expr: patient_id
        data_type: text
      - name: patient_name
        expr: patient_name
        data_type: text
        synonyms: [patient, name]
      - name: gender
        expr: gender
        data_type: text
      - name: age
        expr: age
        data_type: number
      - name: payer_type
        expr: payer_type
        data_type: text
        synonyms: [insurance status, insured, self-pay]
      - name: last_visit_date
        expr: last_visit_date
        data_type: date

  - name: agg_cpt_revenue
    description: >
      Pre-aggregated CPT code revenue and utilization metrics. One row per CPT code.
      Use for procedure-level performance, revenue by code, or reimbursement rate analysis.
    base_table:
      database: cpt_demo
      schema: medical
      table: agg_cpt_revenue
    measures:
      - name: total_charged
        expr: total_charged
        data_type: number
        default_aggregation: sum
        synonyms: [revenue, total billed, gross charges]
      - name: total_reimbursed
        expr: total_reimbursed
        data_type: number
        default_aggregation: sum
      - name: utilization_count
        description: Number of times this CPT code was used
        expr: utilization_count
        data_type: number
        default_aggregation: sum
        synonyms: [usage count, procedure count, frequency]
      - name: reimbursement_rate_pct
        expr: reimbursement_rate_pct
        data_type: number
        default_aggregation: avg
    dimensions:
      - name: cpt_code
        expr: cpt_code
        data_type: text
      - name: description
        expr: description
        data_type: text
        synonyms: [procedure name, service]
      - name: category
        expr: category
        data_type: text
        synonyms: [procedure type, service type]

  - name: agg_disease_billing
    description: >
      Billing aggregated by ICD-10 diagnosis code / disease. One row per diagnosis.
      Use for disease-level revenue, patient count per condition, or cost-per-visit analysis.
    base_table:
      database: cpt_demo
      schema: medical
      table: agg_disease_billing
    measures:
      - name: total_charged
        expr: total_charged
        data_type: number
        default_aggregation: sum
      - name: total_reimbursed
        expr: total_reimbursed
        data_type: number
        default_aggregation: sum
      - name: visit_count
        expr: visit_count
        data_type: number
        default_aggregation: sum
      - name: patient_count
        expr: patient_count
        data_type: number
        default_aggregation: sum
      - name: avg_charge_per_visit
        expr: avg_charge_per_visit
        data_type: number
        default_aggregation: avg
        synonyms: [average cost per visit, cost per encounter]
      - name: reimbursement_rate_pct
        expr: reimbursement_rate_pct
        data_type: number
        default_aggregation: avg
    dimensions:
      - name: diagnosis_code
        expr: diagnosis_code
        data_type: text
        synonyms: [ICD-10, ICD code, diagnosis]
      - name: disease_label
        expr: disease_label
        data_type: text
        synonyms: [disease, condition, diagnosis name]

  - name: agg_monthly_revenue
    description: >
      Month-over-month revenue trend. One row per calendar month.
      Use for revenue trends, seasonal patterns, or monthly performance analysis.
    base_table:
      database: cpt_demo
      schema: medical
      table: agg_monthly_revenue
    measures:
      - name: total_charged
        expr: total_charged
        data_type: number
        default_aggregation: sum
      - name: total_reimbursed
        expr: total_reimbursed
        data_type: number
        default_aggregation: sum
      - name: patient_balance
        expr: patient_balance
        data_type: number
        default_aggregation: sum
      - name: visit_count
        expr: visit_count
        data_type: number
        default_aggregation: sum
      - name: active_patients
        expr: active_patients
        data_type: number
        default_aggregation: sum
        synonyms: [patients seen, unique patients]
    dimensions:
      - name: revenue_month
        expr: revenue_month
        data_type: date
        synonyms: [month, billing month]
      - name: month_label
        expr: month_label
        data_type: text
        synonyms: [month name]

  - name: agg_payer_mix
    description: >
      Insured vs self-pay split. Two rows (Insured / Self-Pay).
      Use for payer mix analysis, collection risk, or revenue by insurance status.
    base_table:
      database: cpt_demo
      schema: medical
      table: agg_payer_mix
    measures:
      - name: total_charged
        expr: total_charged
        data_type: number
        default_aggregation: sum
      - name: total_reimbursed
        expr: total_reimbursed
        data_type: number
        default_aggregation: sum
      - name: patient_balance
        expr: patient_balance
        data_type: number
        default_aggregation: sum
      - name: patient_count
        expr: patient_count
        data_type: number
        default_aggregation: sum
      - name: visit_count
        expr: visit_count
        data_type: number
        default_aggregation: sum
      - name: reimbursement_rate_pct
        expr: reimbursement_rate_pct
        data_type: number
        default_aggregation: avg
      - name: avg_revenue_per_visit
        expr: avg_revenue_per_visit
        data_type: number
        default_aggregation: avg
    dimensions:
      - name: payer_type
        expr: payer_type
        data_type: text
        synonyms: [insurance type, payer category, insured or self-pay]

  - name: agg_provider_metrics
    description: >
      Provider productivity and revenue metrics. One row per provider.
      Use for provider performance, revenue per visit, or provider comparison analysis.
    base_table:
      database: cpt_demo
      schema: medical
      table: agg_provider_metrics
    measures:
      - name: total_charged
        expr: total_charged
        data_type: number
        default_aggregation: sum
      - name: total_reimbursed
        expr: total_reimbursed
        data_type: number
        default_aggregation: sum
      - name: visit_count
        expr: visit_count
        data_type: number
        default_aggregation: sum
      - name: patient_count
        expr: patient_count
        data_type: number
        default_aggregation: sum
      - name: avg_revenue_per_visit
        expr: avg_revenue_per_visit
        data_type: number
        default_aggregation: avg
        synonyms: [revenue per encounter, avg per visit]
      - name: reimbursement_rate_pct
        expr: reimbursement_rate_pct
        data_type: number
        default_aggregation: avg
    dimensions:
      - name: provider_name
        expr: provider_name
        data_type: text
        synonyms: [doctor, physician, provider]
      - name: provider_npi
        expr: provider_npi
        data_type: text

relationships:
  - name: visit_to_patient
    left_table: visits
    right_table: patients
    relationship_columns:
      - left_column: patient_id
        right_column: patient_id
    relationship_type: many_to_one

  - name: procedure_to_visit
    left_table: visit_procedures
    right_table: visits
    relationship_columns:
      - left_column: visit_id
        right_column: visit_id
    relationship_type: many_to_one

  - name: procedure_to_cpt
    left_table: visit_procedures
    right_table: cpt_codes
    relationship_columns:
      - left_column: cpt_code
        right_column: cpt_code
    relationship_type: many_to_one

verified_queries:
  - name: total_billing_summary
    question: What is the total charged versus total reimbursed?
    use_as_onboarding_question: true
    sql: |
      SELECT
        SUM(fee_charged)        AS total_charged,
        SUM(reimbursement)      AS total_reimbursed,
        SUM(patient_balance)    AS total_patient_balance
      FROM cpt_demo.medical.billing_summary

  - name: billing_by_visit
    question: What is the total charged and reimbursed per visit?
    use_as_onboarding_question: true
    sql: |
      SELECT
        visit_id,
        patient_name,
        visit_date,
        SUM(fee_charged)     AS total_charged,
        SUM(reimbursement)   AS total_reimbursed,
        SUM(patient_balance) AS patient_balance
      FROM cpt_demo.medical.billing_summary
      GROUP BY visit_id, patient_name, visit_date
      ORDER BY visit_date

  - name: full_billing_statement
    question: Show full billing summary with patient names
    use_as_onboarding_question: true
    sql: |
      SELECT
        patient_name,
        visit_date,
        visit_type,
        provider_name,
        diagnosis_code,
        cpt_code,
        procedure_description,
        procedure_category,
        units,
        fee_charged,
        reimbursement,
        patient_balance
      FROM cpt_demo.medical.billing_summary
      ORDER BY visit_date, last_name, procedure_id

  - name: patient_billing_summary
    question: Show me a billing summary for each patient
    use_as_onboarding_question: true
    sql: |
      SELECT patient_name, payer_type, visit_count,
             total_charged, total_reimbursed, patient_balance,
             reimbursement_rate_pct
      FROM cpt_demo.medical.agg_patient_billing
      ORDER BY total_charged DESC

  - name: top_cpt_codes
    question: Which CPT codes have the highest revenue?
    use_as_onboarding_question: true
    sql: |
      SELECT cpt_code, description, category, utilization_count,
             total_charged, reimbursement_rate_pct
      FROM cpt_demo.medical.agg_cpt_revenue
      ORDER BY total_charged DESC
      LIMIT 10

  - name: disease_billing
    question: How much was billed for each disease or diagnosis?
    use_as_onboarding_question: true
    sql: |
      SELECT disease_label, diagnosis_code, visit_count, patient_count,
             total_charged, avg_charge_per_visit, reimbursement_rate_pct
      FROM cpt_demo.medical.agg_disease_billing
      ORDER BY total_charged DESC

  - name: monthly_revenue_trend
    question: What is the monthly revenue trend?
    use_as_onboarding_question: true
    sql: |
      SELECT month_label, visit_count, total_charged,
             total_reimbursed, reimbursement_rate_pct
      FROM cpt_demo.medical.agg_monthly_revenue
      ORDER BY revenue_month

  - name: payer_mix
    question: What is the split between insured and self-pay patients?
    use_as_onboarding_question: true
    sql: |
      SELECT payer_type, patient_count, visit_count,
             total_charged, total_reimbursed, patient_balance,
             reimbursement_rate_pct
      FROM cpt_demo.medical.agg_payer_mix

  - name: provider_performance
    question: How does each provider compare in revenue and reimbursement?
    use_as_onboarding_question: true
    sql: |
      SELECT provider_name, visit_count, patient_count,
             total_charged, avg_revenue_per_visit, reimbursement_rate_pct
      FROM cpt_demo.medical.agg_provider_metrics
      ORDER BY total_charged DESC

  - name: self_pay_patients
    question: Which patients are self-pay and what do they owe?
    sql: |
      SELECT patient_name, visit_count, total_charged, patient_balance
      FROM cpt_demo.medical.agg_patient_billing
      WHERE payer_type = 'Self-Pay'
      ORDER BY patient_balance DESC

  - name: diabetes_billing
    question: How much was billed for diabetes patients?
    sql: |
      SELECT disease_label, visit_count, patient_count,
             total_charged, avg_charge_per_visit
      FROM cpt_demo.medical.agg_disease_billing
      WHERE diagnosis_code LIKE 'E11%'
      ORDER BY total_charged DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# TOOL 2 system prompt — Cortex Complete explanation model
# ─────────────────────────────────────────────────────────────────────────────
EXPLAIN_SYSTEM_PROMPT = """You are a medical billing analyst. You receive:
- The user's question
- The SQL that was executed
- The data returned from Snowflake

Write a clear, direct answer that:
1. Directly addresses the question using specific values from the data
   (cite patient names, CPT codes, dollar amounts, dates as appropriate).
2. Highlights key financial figures: gross charges, reimbursements, patient balance.
3. Uses plain English — no SQL terms, no technical jargon.
Keep it factual, concise, and grounded in the data shown."""


# ─────────────────────────────────────────────────────────────────────────────
# Typed result objects
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class AnalystResult:
    """Output of Tool 1: Cortex Analyst NL → SQL."""
    interpretation: str
    sql:            str
    confidence:     dict
    request_id:     str
    latency_ms:     int
    raw_response:   dict = field(default_factory=dict)


@dataclass
class SQLResult:
    """Output of Tool 2: Snowflake SQL Executor."""
    columns:    list
    rows:       list
    row_count:  int
    raw_data:   str        # plain-text table for LLM consumption
    latency_ms: int


@dataclass
class AgentResult:
    """Final output of the full agentic pipeline."""
    question:        str
    interpretation:  str        # Cortex Analyst's plain-English interpretation
    sql:             str        # SQL generated by Cortex Analyst
    columns:         list
    rows:            list
    row_count:       int
    answer:          str        # Cortex Complete explanation
    error:           Optional[str]
    total_latency_ms: int
    tool_latencies:   dict      # {"analyst_ms": ..., "sql_ms": ..., "explain_ms": ...}

    def to_text(self) -> str:
        """Return a human-readable summary (used for --question CLI mode)."""
        parts = [self.answer]
        parts.append(f"\nSQL:\n{self.sql}")
        if self.rows:
            parts.append(f"\nData ({self.row_count} row(s)):")
            parts.append("  ".join(self.columns))
            for row in self.rows[:20]:
                parts.append("  ".join(str(v) for v in row))
        return "\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _clean_account(account: str) -> str:
    suffix = ".snowflakecomputing.com"
    return account[: -len(suffix)] if account.lower().endswith(suffix) else account


def _build_raw_table(columns: list, rows: list, max_rows: int = 20) -> str:
    """Build a plain-text tabular representation suitable for LLM context."""
    if not rows:
        return "(no rows returned)"
    lines = ["  ".join(str(c) for c in columns)]
    for row in rows[:max_rows]:
        lines.append("  ".join(str(v) for v in row))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows) - max_rows} more rows)")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CortexBillingAgent — orchestrates all three tools
# ─────────────────────────────────────────────────────────────────────────────
class CortexBillingAgent:
    """
    Code-first agentic workflow for CPT medical billing NLP queries.

    Tools (all defined in code, no UI):
      1. cortex_analyst  — Cortex Analyst REST API + inline semantic model YAML
      2. sql_executor    — Snowflake Python connector (PAT auth)
      3. cortex_complete — SNOWFLAKE.CORTEX.COMPLETE() via SQL function

    Usage:
        agent = CortexBillingAgent()
        result = agent.run("What is the total patient balance?")
        print(result.answer)
    """

    # ── Tool declarations (code-first) ────────────────────────────────────
    TOOL_REGISTRY = {
        "cortex_analyst": {
            "type":        "cortex_analyst_text_to_sql",
            "description": "Converts natural-language billing questions to Snowflake SQL "
                           "using an inline semantic model for the CPT medical billing schema.",
            "auth":        "programmatic_access_token",
            "endpoint":    "/api/v2/cortex/analyst/message",
            "model":       "inline_semantic_model",
        },
        "sql_executor": {
            "type":        "snowflake_connector_execute",
            "description": "Executes a SELECT statement in Snowflake and returns rows + columns.",
            "auth":        "programmatic_access_token",
            "warehouse":   "COMPUTE_WH",
            "database":    "cpt_demo",
            "schema":      "medical",
        },
        "cortex_complete": {
            "type":        "cortex_complete_sql_function",
            "description": "Generates a plain-English answer using SNOWFLAKE.CORTEX.COMPLETE() "
                           "with the query results as context.",
            "model":       "mistral-large2",
            "max_tokens":  500,
            "temperature": 0,
        },
    }

    def __init__(self) -> None:
        self._account    = _clean_account(os.environ["SNOWFLAKE_ACCOUNT"])
        self._token      = os.environ["SNOWFLAKE_TOKEN"]
        self._warehouse  = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self._analyst_url = (
            f"https://{self._account}.snowflakecomputing.com"
            "/api/v2/cortex/analyst/message"
        )
        self._rest_headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type":  "application/json",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        }

    def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        return snowflake.connector.connect(
            account=self._account,
            user=os.environ["SNOWFLAKE_USER"],
            authenticator="programmatic_access_token",
            token=self._token,
            warehouse=self._warehouse,
            database="cpt_demo",
            schema="medical",
        )

    # ── Tool 1: Cortex Analyst (NL → SQL) ────────────────────────────────
    def _tool_cortex_analyst(self, question: str) -> AnalystResult:
        """
        Calls Cortex Analyst REST API with the inline semantic model.
        Returns the interpreted question + generated SQL.
        """
        t0 = time.time()
        payload = {
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": question}]}
            ],
            "semantic_model": SEMANTIC_MODEL_YAML,
        }
        resp = requests.post(
            self._analyst_url,
            headers=self._rest_headers,
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        latency = int((time.time() - t0) * 1000)

        message  = data.get("message", {})
        content  = message.get("content", [])
        req_id   = data.get("request_id", "")

        interpretation = ""
        sql_statement  = ""
        confidence     = {}

        for block in content:
            btype = block.get("type", "")
            if btype == "text":
                interpretation = block.get("text", "").strip()
            elif btype == "sql":
                sql_statement = block.get("statement", "").strip()
                confidence    = block.get("confidence", {})

        if not sql_statement:
            raise ValueError(
                f"Cortex Analyst returned no SQL. "
                f"Response: {json.dumps(data)[:400]}"
            )

        return AnalystResult(
            interpretation=interpretation,
            sql=sql_statement,
            confidence=confidence,
            request_id=req_id,
            latency_ms=latency,
            raw_response=data,
        )

    # ── Tool 2: Snowflake SQL Executor ────────────────────────────────────
    def _tool_sql_executor(
        self, sql: str, conn: snowflake.connector.SnowflakeConnection
    ) -> SQLResult:
        """
        Executes the SQL against Snowflake via the Python connector.
        Returns columns, rows, and a plain-text table for LLM consumption.
        """
        t0  = time.time()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows    = cur.fetchall()
            columns = [d[0] for d in cur.description] if cur.description else []
        finally:
            cur.close()
        latency  = int((time.time() - t0) * 1000)
        row_list = [list(r) for r in rows]
        raw      = _build_raw_table(columns, row_list)
        return SQLResult(
            columns=columns,
            rows=row_list,
            row_count=len(row_list),
            raw_data=raw,
            latency_ms=latency,
        )

    # ── Tool 3: Cortex Complete explanation ───────────────────────────────
    def _tool_cortex_complete(
        self,
        question: str,
        sql: str,
        sql_result: SQLResult,
        conn: snowflake.connector.SnowflakeConnection,
    ) -> str:
        """
        Calls SNOWFLAKE.CORTEX.COMPLETE() via a SQL function to produce a
        plain-English explanation grounded in the actual query results.
        """
        if not sql_result.rows:
            return "No records matched your query."

        t0 = time.time()
        user_content = (
            f"Question: {question}\n\n"
            f"SQL executed:\n{sql}\n\n"
            f"Query returned {sql_result.row_count} row(s):\n{sql_result.raw_data}"
        )

        # Escape single quotes for the SQL string literal
        sys_escaped  = EXPLAIN_SYSTEM_PROMPT.replace("'", "\\'")
        user_escaped = user_content.replace("'", "\\'")

        explain_sql = f"""
SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'mistral-large2',
  ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT('role', 'system', 'content', '{sys_escaped}'),
    OBJECT_CONSTRUCT('role', 'user',   'content', '{user_escaped}')
  ),
  OBJECT_CONSTRUCT('temperature', 0, 'max_tokens', 500)
) AS answer
"""
        cur = conn.cursor()
        try:
            cur.execute(explain_sql)
            raw = cur.fetchone()[0] or ""
        finally:
            cur.close()

        # Cortex Complete sometimes returns a JSON envelope; unwrap if needed
        answer = raw
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                choices = parsed.get("choices", [])
                if choices:
                    answer = (
                        choices[0].get("messages", "")
                        or choices[0].get("message", {}).get("content", "")
                        or raw
                    )
        except (json.JSONDecodeError, TypeError):
            pass

        latency = int((time.time() - t0) * 1000)
        return answer.strip(), latency

    # ── Orchestration ─────────────────────────────────────────────────────
    def run(self, question: str) -> AgentResult:
        """
        Execute the three-tool pipeline for one natural-language question.

        Workflow:
            question
            → Tool 1 (Cortex Analyst)  → interpretation + SQL
            → Tool 2 (SQL Executor)    → rows + columns
            → Tool 3 (Cortex Complete) → plain-English answer
            → AgentResult
        """
        t_start = time.time()
        error   = None
        analyst_result = None
        sql_result     = None
        answer         = ""
        tool_latencies: dict = {}

        conn = self._get_connection()
        try:
            # ── Step 1: NL → SQL ─────────────────────────────────────────
            analyst_result = self._tool_cortex_analyst(question)
            tool_latencies["analyst_ms"] = analyst_result.latency_ms

            # ── Step 2: Execute SQL ──────────────────────────────────────
            sql_result = self._tool_sql_executor(analyst_result.sql, conn)
            tool_latencies["sql_ms"] = sql_result.latency_ms

            # ── Step 3: Explain results ──────────────────────────────────
            answer, explain_lat = self._tool_cortex_complete(
                question, analyst_result.sql, sql_result, conn
            )
            tool_latencies["explain_ms"] = explain_lat

        except Exception as exc:
            error = str(exc)
            answer = f"ERROR: {exc}"
        finally:
            conn.close()

        return AgentResult(
            question=question,
            interpretation=analyst_result.interpretation if analyst_result else "",
            sql=analyst_result.sql if analyst_result else "",
            columns=sql_result.columns if sql_result else [],
            rows=sql_result.rows    if sql_result else [],
            row_count=sql_result.row_count if sql_result else 0,
            answer=answer,
            error=error,
            total_latency_ms=int((time.time() - t_start) * 1000),
            tool_latencies=tool_latencies,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Public API used by the Express server (--question mode)
# ─────────────────────────────────────────────────────────────────────────────
def run_single_question(question: str) -> str:
    """
    Run one question through the full Cortex pipeline.
    Returns a plain-text answer (used by the API server).
    """
    agent  = CortexBillingAgent()
    result = agent.run(question)
    if result.error:
        return f"ERROR: {result.error}"
    return result.to_text()


# ─────────────────────────────────────────────────────────────────────────────
# CLI — interactive + single-shot mode
# ─────────────────────────────────────────────────────────────────────────────
def _print_banner() -> None:
    w = 72
    print()
    print("─" * w)
    print("  Snowflake Cortex Agentic Workflow — CPT Medical Billing")
    print("  Tools: Cortex Analyst  ·  SQL Executor  ·  Cortex Complete")
    print("  Schema: cpt_demo.medical  ·  Model: mistral-large2")
    print("─" * w)
    print()


def _print_result(result: AgentResult) -> None:
    timing = (
        f"analyst={result.tool_latencies.get('analyst_ms',0)}ms  "
        f"sql={result.tool_latencies.get('sql_ms',0)}ms  "
        f"explain={result.tool_latencies.get('explain_ms',0)}ms  "
        f"total={result.total_latency_ms}ms"
    )
    if result.interpretation:
        print(f"  Interpretation: {result.interpretation}")
    print(f"  SQL: {result.sql[:120]}")
    print(f"  Rows: {result.row_count}  |  {timing}")
    print()
    for line in textwrap.wrap(result.answer, width=70, initial_indent="  ", subsequent_indent="  "):
        print(line)
    print()


def main() -> None:
    # ── Non-interactive mode (--question) ────────────────────────────────
    if "--question" in sys.argv:
        idx = sys.argv.index("--question")
        if idx + 1 >= len(sys.argv):
            print("ERROR: --question requires an argument", file=sys.stderr)
            sys.exit(1)
        question = sys.argv[idx + 1]
        try:
            answer = run_single_question(question)
            print(answer)
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)
        return

    # ── JSON output mode (--json) ─────────────────────────────────────────
    if "--json" in sys.argv:
        idx = sys.argv.index("--json")
        if idx + 1 >= len(sys.argv):
            print("ERROR: --json requires a question argument", file=sys.stderr)
            sys.exit(1)
        question = sys.argv[idx + 1]
        agent  = CortexBillingAgent()
        result = agent.run(question)
        out: dict = {
            "question":       result.question,
            "interpretation": result.interpretation,
            "sql":            result.sql,
            "row_count":      result.row_count,
            "answer":         result.answer,
            "error":          result.error,
            "latency_ms":     result.total_latency_ms,
            "tool_latencies": result.tool_latencies,
        }
        print(json.dumps(out, default=str))
        return

    # ── Interactive REPL ──────────────────────────────────────────────────
    _print_banner()
    agent = CortexBillingAgent()
    print("  Initialising Cortex agent…", end="", flush=True)
    print(" ready.\n")

    EXAMPLES = [
        "Show all patients",
        "What procedures were billed for visit VIS-001?",
        "What is the total charged vs reimbursed per visit?",
        "Which patient had the most expensive visit?",
        "Show the full billing summary with patient names",
        "What is the patient balance for each visit?",
        "Which CPT codes have the highest reimbursement rate?",
    ]
    print("  Example questions:")
    for i, ex in enumerate(EXAMPLES, 1):
        print(f"    {i}. {ex}")
    print()

    while True:
        try:
            question = input("  You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not question:
            continue
        if question.lower() in ("exit", "quit", "q"):
            break

        print("  Running pipeline…", end="", flush=True)
        result = agent.run(question)
        print()
        if result.error:
            print(f"  ERROR: {result.error}\n")
        else:
            _print_result(result)

    print("  Goodbye!\n")


if __name__ == "__main__":
    main()
