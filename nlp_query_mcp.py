"""
Natural Language Query Interface — powered by Snowflake MCP Server + OpenAI

Architecture:
  User question
      ↓
  OpenAI (gpt-5.1) with MCP tools
      ↓  tool_calls (list_tables / describe_table / read_query)
  Snowflake MCP Server (subprocess via stdio)
      ↓  executes against Snowflake
  Results returned to OpenAI
      ↓
  Plain-English answer displayed
"""

import asyncio
import json
import os
import sys
import textwrap

from openai import OpenAI
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

# ── Colors ────────────────────────────────────────────────────────────────────

C = {
    "reset":  "\033[0m",  "bold":  "\033[1m",
    "cyan":   "\033[96m", "green": "\033[92m",
    "yellow": "\033[93m", "red":   "\033[91m",
    "grey":   "\033[90m", "blue":  "\033[94m",
    "white":  "\033[97m", "magenta": "\033[95m",
}

def c(text, *styles):
    return "".join(C[s] for s in styles) + text + C["reset"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean_account(account: str) -> str:
    suffix = ".snowflakecomputing.com"
    if account.lower().endswith(suffix):
        account = account[: -len(suffix)]
    return account

def snowflake_credentials() -> dict:
    return {
        "account":       _clean_account(os.environ["SNOWFLAKE_ACCOUNT"]),
        "user":          os.environ["SNOWFLAKE_USER"],
        "authenticator": "programmatic_access_token",
        "token":         os.environ["SNOWFLAKE_TOKEN"],
        "warehouse":     os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database":      os.environ.get("SNOWFLAKE_DATABASE", "cpt_demo"),
        "schema":        os.environ.get("SNOWFLAKE_SCHEMA",   "medical"),
    }

def openai_client() -> OpenAI:
    return OpenAI(
        base_url=os.environ["AI_INTEGRATIONS_OPENAI_BASE_URL"],
        api_key=os.environ["AI_INTEGRATIONS_OPENAI_API_KEY"],
    )

# ── MCP tools → OpenAI function schema ────────────────────────────────────────

def mcp_tools_to_openai(mcp_tools) -> list[dict]:
    """Convert MCP tool definitions to OpenAI function-calling schema."""
    return [
        {
            "type": "function",
            "function": {
                "name":        tool.name,
                "description": tool.description,
                "parameters":  tool.inputSchema,
            },
        }
        for tool in mcp_tools
    ]

# ── Result formatting ─────────────────────────────────────────────────────────

def format_tool_result(result_text: str) -> str:
    """Parse YAML/JSON tool result and return a compact table string."""
    try:
        import yaml
        parsed = yaml.safe_load(result_text)
        if isinstance(parsed, dict) and "data" in parsed:
            rows = parsed["data"]
            if not rows:
                return "(no rows)"
            cols = list(rows[0].keys())
            widths = [min(max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows)), 28) for c in cols]
            header  = " | ".join(str(col)[:28].ljust(widths[i]) for i, col in enumerate(cols))
            divider = "-+-".join("-" * w for w in widths)
            lines   = [header, divider]
            for row in rows:
                lines.append(" | ".join(str(row.get(col, ""))[:28].ljust(widths[i]) for i, col in enumerate(cols)))
            lines.append(f"\n({len(rows)} row(s))")
            return "\n".join(lines)
    except Exception:
        pass
    return result_text[:2000]

# ── Agentic loop ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a medical billing data assistant connected to a Snowflake database.
Database: cpt_demo   Schema: medical

You have tools to explore and query the database:
- list_tables      — list all tables in the schema
- describe_table   — get column details for a table
- read_query       — run a SELECT query

When the user asks a question:
1. Use list_tables / describe_table if you need to understand the schema.
2. Use read_query to fetch the data.
3. Summarise the results in plain English — be concise and accurate.
4. Do NOT invent data. Only report what the query returns.
5. Never attempt write operations.
"""

async def agent_turn(
    ai: OpenAI,
    session: ClientSession,
    mcp_tools,
    openai_tools: list[dict],
    messages: list[dict],
    question: str,
) -> str:
    """One full agentic turn: user question → tool calls → final answer."""

    messages.append({"role": "user", "content": question})

    while True:
        response = ai.chat.completions.create(
            model="gpt-5.1",
            max_completion_tokens=2048,
            messages=messages,
            tools=openai_tools,
            tool_choice="auto",
        )

        msg = response.choices[0].message
        stop = response.choices[0].finish_reason

        # No tool calls → final answer
        if stop == "stop" or not msg.tool_calls:
            answer = msg.content or ""
            messages.append({"role": "assistant", "content": answer})
            return answer

        # Append assistant tool-call message
        messages.append({
            "role":       "assistant",
            "content":    msg.content,
            "tool_calls": [
                {
                    "id":       tc.id,
                    "type":     "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in msg.tool_calls
            ],
        })

        # Execute each tool call against the MCP server
        for tc in msg.tool_calls:
            tool_name = tc.function.name
            try:
                args = json.loads(tc.function.arguments) if tc.function.arguments else {}
            except json.JSONDecodeError:
                args = {}

            print(c(f"    → calling tool: ", "grey") + c(tool_name, "yellow") +
                  (c(f"({args})", "grey") if args else ""))

            try:
                result = await session.call_tool(tool_name, args)
                # Extract text from MCP result content
                text_parts = [c.text for c in result.content if hasattr(c, "text")]
                tool_output = "\n".join(text_parts)
            except Exception as e:
                tool_output = f"Error calling {tool_name}: {e}"

            # Show a formatted preview
            print(c(format_tool_result(tool_output), "white"))
            print()

            messages.append({
                "role":         "tool",
                "tool_call_id": tc.id,
                "content":      tool_output,
            })

# ── Banner & help ─────────────────────────────────────────────────────────────

def print_banner():
    print()
    print(c("╔══════════════════════════════════════════════════════════════╗", "blue", "bold"))
    print(c("║   CPT Medical Billing — MCP + NLP Query Interface            ║", "blue", "bold"))
    print(c("║   Powered by: Snowflake MCP Server  ·  OpenAI gpt-5.1        ║", "blue"))
    print(c("║   Database: cpt_demo  ·  Schema: medical                     ║", "blue"))
    print(c("╚══════════════════════════════════════════════════════════════╝", "blue", "bold"))
    print()
    print(c("  Ask anything in plain English.  ", "grey") +
          c("help", "yellow") + c(" for examples  ·  ", "grey") +
          c("exit", "yellow") + c(" to quit\n", "grey"))

EXAMPLES = [
    "What tables exist in this database?",
    "Describe the visit_procedures table",
    "Show all patients",
    "List all CPT codes with their fees",
    "What procedures were billed for visit VIS-001?",
    "What is the total charged and reimbursed per visit?",
    "Which CPT code category has the highest base fee?",
    "Show the full billing summary with patient names",
    "What was the reimbursement rate for James Carter's visit?",
    "Are there any visits without procedures?",
]

def print_help():
    print(c("\n  Example questions:\n", "yellow"))
    for i, ex in enumerate(EXAMPLES, 1):
        print(f"  {c(str(i)+'.', 'grey')} {ex}")
    print()

# ── Main ──────────────────────────────────────────────────────────────────────

async def run_single_question(question: str) -> str:
    """Run one question non-interactively and return the answer string."""
    wrapper = os.path.join(os.path.dirname(__file__), "mcp_server_wrapper.py")
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[wrapper],
        env={**os.environ},
    )
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            mcp_tools_list = (await session.list_tools()).tools
            openai_tools   = mcp_tools_to_openai(mcp_tools_list)
            ai             = openai_client()
            messages       = [{"role": "system", "content": SYSTEM_PROMPT}]
            return await agent_turn(
                ai, session, mcp_tools_list, openai_tools, messages, question
            )


async def run():
    # ── Non-interactive API mode ──────────────────────────────────────────
    if "--question" in sys.argv:
        idx = sys.argv.index("--question")
        if idx + 1 >= len(sys.argv):
            print("ERROR: --question requires an argument", file=sys.stderr)
            sys.exit(1)
        question = sys.argv[idx + 1]
        answer = await run_single_question(question)
        print(answer)
        return

    # ── Interactive CLI mode ──────────────────────────────────────────────
    print_banner()

    creds = snowflake_credentials()

    # Build MCP server launch parameters using the token-auth wrapper
    wrapper = os.path.join(os.path.dirname(__file__), "mcp_server_wrapper.py")
    server_params = StdioServerParameters(
        command=sys.executable,
        args=[wrapper],
        env={**os.environ},
    )

    print(c("  Starting Snowflake MCP server...", "grey"), end="", flush=True)

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            mcp_tools_list = (await session.list_tools()).tools
            openai_tools    = mcp_tools_to_openai(mcp_tools_list)
            tool_names      = [t.name for t in mcp_tools_list]
            print(c(f" ready. Tools: {', '.join(tool_names)}\n", "green"))

            ai       = openai_client()
            messages = [{"role": "system", "content": SYSTEM_PROMPT}]

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
                    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
                    print(c("  Conversation history cleared.\n", "grey"))
                    continue

                print(c("  Thinking...\n", "grey"))
                try:
                    answer = await agent_turn(
                        ai, session, mcp_tools_list, openai_tools, messages, question
                    )
                    # Wrap long answer lines
                    for line in answer.splitlines():
                        print(c("  ", "reset") + textwrap.fill(
                            line, width=74,
                            initial_indent="  " if not line.startswith("  ") else "",
                            subsequent_indent="    ",
                        ))
                    print()
                except Exception as e:
                    print(c(f"  Error: {e}\n", "red"))

    print(c("\n  MCP server stopped. Goodbye!\n", "grey"))


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
