import os
import snowflake.connector

def _clean_account(account: str) -> str:
    suffix = ".snowflakecomputing.com"
    if account.lower().endswith(suffix):
        account = account[: -len(suffix)]
    return account

def get_connection():
    conn = snowflake.connector.connect(
        account=_clean_account(os.environ["SNOWFLAKE_ACCOUNT"]),
        user=os.environ["SNOWFLAKE_USER"],
        authenticator="programmatic_access_token",
        token=os.environ["SNOWFLAKE_TOKEN"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
        database=os.environ.get("SNOWFLAKE_DATABASE", ""),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", ""),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )
    return conn

def main():
    print("Connecting to Snowflake...")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"Connected successfully! Snowflake version: {version[0]}")

        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
        row = cursor.fetchone()
        print(f"User:      {row[0]}")
        print(f"Role:      {row[1]}")
        print(f"Database:  {row[2]}")
        print(f"Schema:    {row[3]}")
        print(f"Warehouse: {row[4]}")
    finally:
        cursor.close()
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
