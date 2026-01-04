import os, time, random, argparse
from decimal import Decimal, ROUND_DOWN
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

DDL = """
CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  account_type VARCHAR(50) NOT NULL,
  balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
  id BIGSERIAL PRIMARY KEY,
  account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  txn_type VARCHAR(50) NOT NULL,
  amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
  related_account_id INT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
  created_at TIMESTAMPTZ DEFAULT now()
);
"""

def money(min_v, max_v):
    v = Decimal(str(random.uniform(float(min_v), float(max_v))))
    return v.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def connect_with_retry(attempts=30, sleep_s=2):
    for i in range(1, attempts + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
            )
            conn.autocommit = False
            print(f"✅ Connected to Postgres ({os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')})")
            return conn
        except Exception as e:
            print(f"⏳ Postgres not ready ({i}/{attempts})... {e}")
            time.sleep(sleep_s)
    raise RuntimeError("❌ Could not connect to Postgres after retries")

def ensure_tables(cur):
    cur.execute(DDL)

def run_iteration(cur, n_customers, accounts_per_customer, n_txns, currency):
    customers, accounts = [], []

    for _ in range(n_customers):
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s,%s,%s) RETURNING id",
            (fake.first_name(), fake.last_name(), fake.unique.email()),
        )
        customers.append(cur.fetchone()[0])

    for cid in customers:
        for _ in range(accounts_per_customer):
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s,%s,%s,%s) RETURNING id",
                (cid, random.choice(["SAVINGS","CHECKING"]), money(Decimal("10"), Decimal("1000")), currency),
            )
            accounts.append(cur.fetchone()[0])

    for _ in range(n_txns):
        account_id = random.choice(accounts)
        txn_type = random.choice(["DEPOSIT","WITHDRAWAL","TRANSFER"])
        related = None
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related = random.choice([a for a in accounts if a != account_id])

        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s,%s,%s,%s,'COMPLETED')",
            (account_id, txn_type, money(Decimal("1"), Decimal("1000")), related),
        )

    return len(customers), len(accounts), n_txns

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true")
    args = p.parse_args()

    sleep_s = int(os.getenv("SLEEP_SECONDS", "2"))
    n_customers = int(os.getenv("NUM_CUSTOMERS", "10"))
    accounts_per = int(os.getenv("ACCOUNTS_PER_CUSTOMER", "2"))
    n_txns = int(os.getenv("NUM_TRANSACTIONS", "50"))
    currency = os.getenv("CURRENCY", "USD")

    conn = connect_with_retry()
    iterations = 0

    try:
        with conn.cursor() as cur:
            ensure_tables(cur)
        conn.commit()
        print("✅ Tables ensured (customers, accounts, transactions)")

        while True:
            iterations += 1
            try:
                with conn.cursor() as cur:
                    c, a, t = run_iteration(cur, n_customers, accounts_per, n_txns, currency)
                conn.commit()
                print(f"✅ Iteration {iterations}: customers={c}, accounts={a}, txns={t}")
            except Exception as e:
                conn.rollback()
                print(f"❌ Iteration {iterations} failed: {e}")

            if args.once:
                break
            time.sleep(sleep_s)

    except KeyboardInterrupt:
        print("\n👋 Stopped by user (Ctrl+C).")

    finally:
        conn.close()
        print(f"✅ Producer exited cleanly after {iterations} iterations.")

if __name__ == "__main__":
    main()
