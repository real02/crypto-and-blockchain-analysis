import snowflake.connector

SNOWFLAKE_CONN = {
    "user": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",  # e.g. xy12345.eu-west-1
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "role": "YOUR_ROLE",  # optional but recommended
}


def load_cex_addresses_from_snowflake():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN, schema="RAW")
    cs = conn.cursor()

    cs.execute("SELECT DISTINCT LOWER(address), exchange FROM cex_addresses")
    rows = cs.fetchall()
    cs.close()
    conn.close()

    # # Option 1: Return just a list of addresses
    return [r[0] for r in rows]

    # Option 2 (if you want attribution):
    # return [{"exchange": entity, "address": address} for address, entity in rows].setdefault(token.lower(), []).append(address)
