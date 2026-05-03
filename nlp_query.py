import os
import sys
import textwrap
import snowflake.connector
from openai import OpenAI

SCHEMA_CONTEXT = """
You are a SQL expert assistant for a medical billing database in Snowflake.
Database: cpt_demo   Schema: medical

Tables:

1. patients
   - patient_id   VARCHAR(20) PK
   - first_name   VARCHAR(50)
   - last_name    VARCHAR(50)
   - date_of_birth DATE
   - age          INT
   - gender       VARCHAR(10)
   - insurance_id VARCHAR(30)

2. cpt_codes
   - cpt_code    VARCHAR(10) PK
   - description VARCHAR(255)
   - category    VARCHAR(50)   -- e.g. 'Evaluation & Management', 'Vaccine', 'Administration'
   - base_fee    DECIMAL(10,2)

3. visits
   - visit_id       VARCHAR(20) PK
   - patient_id     VARCHAR(20) FK -> patients
   - visit_date     DATE
   - visit_type     VARCHAR(50)
   - provider_name  VARCHAR(100)
   - provider_npi   VARCHAR(20)
   - diagnosis_code VARCHAR(20)
   - notes          VARCHAR(500)

4. visit_procedures
   - procedure_id  VARCHAR(20) PK
   - visit_id      VARCHAR(20) FK -> visits
   - cpt_code      VARCHAR(10) FK -> cpt_codes
   - units         INT
   - fee_charged   DECIMAL(10,2)
   - reimbursement DECIMAL(10,2)
   - notes         VARCHAR(255)

Rules:
- Return ONLY a single valid Snowflake SQL SELECT query. No explanations. No markdown fences.
- Never generate INSERT, UPDATE, DELETE, DROP, TRUNCATE, or DDL statements.
- If the question cannot be answered with a SELECT query, respond with: ERROR: <reason>
- Always qualify table names (e.g. patients, cpt_codes, visits, visit_procedures).
- Use proper Snowflake SQL syntax (e.g. || for string concat).
"""

COLORS = {
    "reset":  "\033[0m",
    "bold":   "\033[1m",
    "cyan":   "\033[96m",
    "green":  "\033[92m",
    "yellow": "\033[93m",
    "red":    "\033[91m",
    "grey":   "\033[90m",
    "blue":   "\033[94m",
    "white":  "\033[97m",
}

def c(text, *styles):
    return "".join(COLORS[s] for s in styles) + text + COLORS["reset"]

def _clean_account(account):
    suffix = ".snowflakecomputing.com"
    if account.lower().endswith(suffix):
        account = account[: -len(suffix)]
    return account

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=_clean_account(os.environ["SNOWFLAKE_ACCOUNT"]),
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="cpt_demo",
        schema="medical",
    )

def get_openai_client():
    return OpenAI(
        base_url=os.environ["AI_INTEGRATIONS_OPENAI_BASE_URL"],
        api_key=os.environ["AI_INTEGRATIONS_OPENAI_API_KEY"],
    )

def natural_language_to_sql(client, question, history):
    messages = [{"role": "system", "content": SCHEMA_CONTEXT}]
    messages.extend(history)
    messages.append({"role": "user", "content": question})

    response = client.chat.completions.create(
        model="gpt-5.1",
        max_completion_tokens=512,
        messages=messages,
    )
    return response.choices[0].message.content.strip()

def explain_results(client, question, sql, rows, cols):
    if not rows:
        return "No records matched your query."
    # Build a full data preview (up to 10 rows so the model can cite actual values)
    preview = "  ".join(cols) + "\n"
    for row in rows[:10]:
        preview += "  ".join(str(v) for v in row) + "\n"
    prompt = (
        f"The user asked: \"{question}\"\n"
        f"SQL executed:\n{sql}\n\n"
        f"Query returned {len(rows)} row(s):\n{preview}\n"
        "Write a clear, direct answer that:\n"
        "1. Directly answers the question using specific values from the data above "
        "(cite patient names, CPT codes, dollar amounts, dates, counts — whatever is relevant).\n"
        "2. Highlights any key financial figures: charges, reimbursements, patient balances.\n"
        "3. Uses plain English; no SQL jargon.\n"
        "Keep the response focused and factual — no filler phrases like 'the data shows' or 'it lists'."
    )
    response = client.chat.completions.create(
        model="gpt-5.1",
        max_completion_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content.strip()

def print_results(rows, cols):
    if not rows:
        print(c("  (no rows returned)", "grey"))
        return

    col_widths = [len(str(col)) for col in cols]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    col_widths = [min(w, 30) for w in col_widths]

    header = "  " + " | ".join(c(str(col)[:30].ljust(col_widths[i]), "bold", "cyan") for i, col in enumerate(cols))
    divider = "  " + "-+-".join("-" * col_widths[i] for i in range(len(cols)))

    print(header)
    print(c(divider, "grey"))
    for row in rows:
        line = "  " + " | ".join(str(v)[:30].ljust(col_widths[i]) for i, v in enumerate(row))
        print(line)
    print(c(f"\n  {len(rows)} row(s) returned.", "grey"))

def print_banner():
    print()
    print(c("╔══════════════════════════════════════════════════════════════╗", "blue"))
    print(c("║   CPT Medical Billing — Natural Language Query Interface     ║", "blue"))
    print(c("║   Database: cpt_demo  ·  Schema: medical                     ║", "blue"))
    print(c("╚══════════════════════════════════════════════════════════════╝", "blue"))
    print()
    print(c("  Ask questions in plain English. Type ", "grey") +
          c("help", "yellow") + c(" for examples, ", "grey") +
          c("exit", "yellow") + c(" to quit.\n", "grey"))

EXAMPLES = [
    "Show all patients",
    "List all CPT codes and their fees",
    "What procedures were billed for visit VIS-001?",
    "What is the total charged vs reimbursed per visit?",
    "Which patient had the most expensive visit?",
    "Show all vaccine-related CPT codes",
    "What diagnosis code was used for James Carter's visit?",
    "Show the full billing summary with patient names and procedure details",
]

def print_help():
    print(c("\n  Example questions you can ask:\n", "yellow"))
    for i, ex in enumerate(EXAMPLES, 1):
        print(f"  {c(str(i) + '.', 'grey')} {ex}")
    print()

def run_single_question(question: str) -> str:
    """Run one question non-interactively and return a plain-English answer."""
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        ai = get_openai_client()
        sql = natural_language_to_sql(ai, question, [])
        if sql.startswith("ERROR:"):
            return sql
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        explanation = explain_results(ai, question, sql, rows, cols)
        row_lines = []
        if rows:
            row_lines.append("  ".join(str(c) for c in cols))
            for row in rows:
                row_lines.append("  ".join(str(v) for v in row))
        data_block = "\n".join(row_lines) if row_lines else "(no rows)"
        return f"{explanation}\n\nSQL: {sql}\n\nData:\n{data_block}"
    finally:
        cur.close()
        conn.close()


def main():
    # ── Non-interactive API mode ──────────────────────────────────────────
    if "--question" in sys.argv:
        idx = sys.argv.index("--question")
        if idx + 1 >= len(sys.argv):
            print("ERROR: --question requires an argument", file=sys.stderr)
            sys.exit(1)
        question = sys.argv[idx + 1]
        try:
            answer = run_single_question(question)
            print(answer)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        return

    # ── Interactive CLI mode ──────────────────────────────────────────────
    print_banner()

    print(c("  Connecting to Snowflake...", "grey"), end="", flush=True)
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        print(c(" connected.", "green"))
    except Exception as e:
        print(c(f" FAILED: {e}", "red"))
        sys.exit(1)

    print(c("  Loading AI...", "grey"), end="", flush=True)
    try:
        ai = get_openai_client()
        print(c(" ready.\n", "green"))
    except Exception as e:
        print(c(f" FAILED: {e}", "red"))
        cur.close()
        conn.close()
        sys.exit(1)

    history = []

    try:
        while True:
            try:
                question = input(c("  You: ", "cyan", "bold")).strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if not question:
                continue

            low = question.lower()
            if low in ("exit", "quit", "bye", "q"):
                break
            if low in ("help", "?", "examples"):
                print_help()
                continue
            if low in ("clear", "reset"):
                history.clear()
                print(c("  Conversation history cleared.\n", "grey"))
                continue

            print(c("  Generating SQL...", "grey"), end="", flush=True)
            try:
                sql = natural_language_to_sql(ai, question, history)
            except Exception as e:
                print(c(f" ERROR: {e}\n", "red"))
                continue

            if sql.startswith("ERROR:"):
                print()
                print(c(f"  {sql}\n", "red"))
                continue

            print(c(" done.", "grey"))
            print(c(f"\n  SQL: ", "grey") + c(sql, "yellow") + "\n")

            try:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                print_results(rows, cols)

                history.append({"role": "user", "content": question})
                history.append({"role": "assistant", "content": sql})
                if len(history) > 20:
                    history = history[-20:]

                print(c("\n  Insight: ", "green", "bold"), end="")
                try:
                    explanation = explain_results(ai, question, sql, rows, cols)
                    print(textwrap.fill(explanation, width=72,
                                        initial_indent="", subsequent_indent="          "))
                except Exception:
                    pass

            except Exception as e:
                print(c(f"\n  Query error: {e}\n", "red"))

            print()

    finally:
        cur.close()
        conn.close()
        print(c("\n  Connection closed. Goodbye!\n", "grey"))

if __name__ == "__main__":
    main()
