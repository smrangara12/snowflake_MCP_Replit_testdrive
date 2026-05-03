# CPT Medical Billing — Snowflake NLP Query System

A Python-based system that connects to Snowflake and allows querying medical billing data through natural language, powered by an MCP (Model Context Protocol) server and OpenAI.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         REPLIT ENVIRONMENT                                  │
│                                                                             │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────────┐  │
│  │   API Server     │    │  Mockup Sandbox  │    │   Python Scripts     │  │
│  │ artifacts/       │    │ artifacts/       │    │ scripts/             │  │
│  │ api-server       │    │ mockup-sandbox   │    │ nlp_query_mcp.py     │  │
│  │ Path: /api       │    │ Path: /__mockup  │    │ nlp_query.py         │  │
│  │ Express 5        │    │ Vite preview     │    │ cpt_coding_run.py    │  │
│  └──────────────────┘    └──────────────────┘    └──────────┬───────────┘  │
│                                                             │               │
└─────────────────────────────────────────────────────────────┼───────────────┘
                                                              │
                              ┌───────────────────────────────▼──────────────┐
                              │           AUTH & CONNECTION LAYER             │
                              │                                               │
                              │  Replit Secrets:                              │
                              │    SNOWFLAKE_ACCOUNT  (account identifier)    │
                              │    SNOWFLAKE_USER     (username)              │
                              │    SNOWFLAKE_TOKEN    (programmatic token)    │
                              │    AI_INTEGRATIONS_OPENAI_BASE_URL            │
                              │    AI_INTEGRATIONS_OPENAI_API_KEY             │
                              └───────────────────────────────┬──────────────┘
                                                              │
                              ┌───────────────────────────────▼──────────────┐
                              │         SNOWFLAKE CLOUD                       │
                              │  Database: cpt_demo  ·  Schema: medical       │
                              │  Warehouse: COMPUTE_WH                        │
                              │                                               │
                              │  ┌─────────────┐  ┌──────────────────────┐   │
                              │  │  patients   │  │      cpt_codes       │   │
                              │  └─────────────┘  └──────────────────────┘   │
                              │  ┌─────────────┐  ┌──────────────────────┐   │
                              │  │   visits    │  │  visit_procedures    │   │
                              │  └─────────────┘  └──────────────────────┘   │
                              └──────────────────────────────────────────────┘
```

---

## NLP Application Stack (MCP-based)

```
User types a question
        │
        ▼
┌───────────────────┐
│  nlp_query_mcp.py │   OpenAI gpt-5.1 with tool_choice="auto"
│  (agentic loop)   │◄──────────────────────────────────────────┐
└────────┬──────────┘                                           │
         │  MCP stdio transport                                  │
         ▼                                                       │
┌────────────────────────┐                                      │
│  mcp_server_wrapper.py │   Patches: package name lookup,      │
│  (subprocess)          │   cursor API, Decimal serialization  │
└────────┬───────────────┘                                      │
         │  Snowflake connector (token auth)                     │
         ▼                                                       │
┌────────────────────────┐    Tool results (YAML/JSON)          │
│  Snowflake Cloud       │─────────────────────────────────────►│
│  cpt_demo.medical      │
└────────────────────────┘

MCP Tools exposed:
  • list_tables     — discover all tables in the schema
  • describe_table  — inspect columns of any table
  • read_query      — execute a SELECT query (write-protected)
  • append_insight  — save findings to an in-memory memo
```

---

## Artifacts

### 1. API Server (`artifacts/api-server`)
- **Framework:** Express 5 + TypeScript
- **Port:** `$PORT` (assigned by Replit)
- **Preview path:** `/api`
- **Purpose:** Backend API layer for any web-based features
- **Run:** `pnpm --filter @workspace/api-server run dev`

### 2. Mockup Sandbox (`artifacts/mockup-sandbox`)
- **Framework:** Vite
- **Port:** `$PORT` (assigned by Replit)
- **Preview path:** `/__mockup`
- **Purpose:** Canvas-based component preview server
- **Run:** `pnpm --filter @workspace/mockup-sandbox run dev`

---

## Scripts

| File | Purpose |
|------|---------|
| `scripts/snowflake_connect.py` | Baseline connection test — verifies credentials and prints session info |
| `scripts/cpt_coding_setup.sql` | DDL + sample data: 4 tables, 3 CPT codes, 1 patient, 1 visit, 3 procedures |
| `scripts/cpt_coding_run.py` | Runs setup SQL then queries and prints all tables |
| `scripts/nlp_query.py` | NLP interface v1 — pre-generated SQL from schema prompt |
| `scripts/nlp_query_mcp.py` | NLP interface v2 — agentic MCP loop (recommended) |
| `scripts/mcp_server_wrapper.py` | MCP server subprocess with token-auth patches |

---

## Database Schema

### `patients`
| Column | Type | Notes |
|--------|------|-------|
| patient_id | VARCHAR(20) | PK |
| first_name | VARCHAR(50) | |
| last_name | VARCHAR(50) | |
| date_of_birth | DATE | |
| age | INT | |
| gender | VARCHAR(10) | |
| insurance_id | VARCHAR(30) | |

### `cpt_codes`
| Column | Type | Notes |
|--------|------|-------|
| cpt_code | VARCHAR(10) | PK |
| description | VARCHAR(255) | |
| category | VARCHAR(50) | e.g. Evaluation & Management, Vaccine, Administration |
| base_fee | DECIMAL(10,2) | |

### `visits`
| Column | Type | Notes |
|--------|------|-------|
| visit_id | VARCHAR(20) | PK |
| patient_id | VARCHAR(20) | FK → patients |
| visit_date | DATE | |
| visit_type | VARCHAR(50) | |
| provider_name | VARCHAR(100) | |
| provider_npi | VARCHAR(20) | |
| diagnosis_code | VARCHAR(20) | ICD-10 code |
| notes | VARCHAR(500) | |

### `visit_procedures`
| Column | Type | Notes |
|--------|------|-------|
| procedure_id | VARCHAR(20) | PK |
| visit_id | VARCHAR(20) | FK → visits |
| cpt_code | VARCHAR(10) | FK → cpt_codes |
| units | INT | default 1 |
| fee_charged | DECIMAL(10,2) | |
| reimbursement | DECIMAL(10,2) | |
| notes | VARCHAR(255) | |

---

## Sample Data — CPT Coding Scenario

**Scenario:** 30-year-old patient, preventive visit + Hepatitis B vaccine

| CPT Code | Description | Charged | Reimbursed |
|----------|-------------|---------|------------|
| 99395 | Preventive visit, established patient, age 18–39 | $185.00 | $148.00 |
| 90739 | Hepatitis B vaccine, adult dosage | $65.00 | $52.00 |
| 90471 | Immunization administration, 1st injection | $25.00 | $20.00 |
| | **Total** | **$275.00** | **$220.00** |

- **Patient:** James Carter, DOB 1994-03-15, Age 30, Male
- **Visit:** VIS-001, 2024-06-10, Dr. Sarah Nguyen (NPI: 1234567890)
- **Diagnosis:** Z00.00 (encounter for general adult medical examination)

---

## Environment Variables / Secrets

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Account identifier (e.g. `XY12345-AB12345`) — `.snowflakecomputing.com` stripped automatically |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_TOKEN` | Programmatic access token (generated in Snowflake UI) |
| `SNOWFLAKE_WAREHOUSE` | *(optional)* Defaults to `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | *(optional)* Defaults to `cpt_demo` |
| `SNOWFLAKE_SCHEMA` | *(optional)* Defaults to `medical` |
| `AI_INTEGRATIONS_OPENAI_BASE_URL` | Auto-set by Replit AI integration |
| `AI_INTEGRATIONS_OPENAI_API_KEY` | Auto-set by Replit AI integration |

---

## How to Test

### 1. Verify Snowflake connection
```bash
cd scripts && python snowflake_connect.py
```
Expected output:
```
Connecting to Snowflake...
Connected successfully! Snowflake version: 10.x.x
User:      <your user>
Role:      <your role>
Warehouse: COMPUTE_WH
Connection closed.
```

### 2. Load the schema and sample data
```bash
cd scripts && python cpt_coding_run.py
```
Expected output: Creates 4 tables, inserts sample data, then prints all tables and a billing summary.

### 3. Run the NLP query app (MCP-based — recommended)
```bash
cd scripts && python nlp_query_mcp.py
```
Try these questions:
```
What tables exist in this database?
Describe the visit_procedures table
Show all patients
What procedures were billed for visit VIS-001?
What is the total charged and reimbursed per visit?
Which CPT code category has the highest base fee?
Show the full billing summary with patient names
```
Type `help` for more examples. Type `exit` to quit.

### 4. Run the basic NLP query app (direct SQL generation)
```bash
cd scripts && python nlp_query.py
```
Same interface, but uses a hardcoded schema prompt instead of MCP tool discovery.

### 5. Test MCP server wrapper directly
```bash
cd scripts && python mcp_server_wrapper.py
```
The server starts and waits for MCP protocol messages on stdio (Ctrl+C to stop).

---

## Dependencies

### Python packages
```
snowflake-connector-python==4.4.0
mcp==1.27.0
mcp-server-snowflake==0.3.8
openai (latest)
```

Install:
```bash
pip install snowflake-connector-python mcp mcp-server-snowflake openai
```

### Node.js packages
Managed via pnpm workspaces — run `pnpm install` from the project root.

---

## Key Design Decisions

| Decision | Reason |
|----------|--------|
| Programmatic access token auth | Avoids MFA requirement on the Snowflake account |
| MCP server as subprocess (stdio) | Clean separation — MCP server handles Snowflake; our app handles the UI and AI loop |
| OpenAI tool_choice="auto" | AI decides when to explore schema vs run queries — no hardcoded SQL generation |
| Decimal → float conversion | Snowflake returns `DECIMAL` types that aren't JSON-serializable by default |
| `_clean_account()` helper | Some accounts include `.snowflakecomputing.com` suffix; the connector adds it automatically |
