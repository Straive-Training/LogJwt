import sqlite3
import pandas as pd

DB_FILE = "loan_app.db"
TABLE_NAME = "accounts"

def create_database_and_table():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                account_id INTEGER PRIMARY KEY,
                account_name TEXT NOT NULL,
                account_type TEXT NOT NULL,
                balance REAL NOT NULL,
                pan_number TEXT,
                tan_number TEXT
            )
        """)

        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        conn.close()


def insert_account(account):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (account_id, account_name, account_type, balance, pan_number, tan_number)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            account['account_id'],
            account['account_name'],
            account['account_type'],
            account['balance'],
            account.get('pan_number'),
            account.get('tan_number')
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        return False
    except Exception as e:
        print(f"Error inserting account: {e}")
        return False
    finally:
        conn.close()


def get_all_accounts():
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        return df
    except Exception as e:
        print(f"Error fetching all accounts: {e}")
        return pd.DataFrame()  # return empty DataFrame on failure
    finally:
        conn.close()


def get_account_by_id(account_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE account_id=?", (account_id,))
        row = cursor.fetchone()
        return row
    except Exception as e:
        print(f"Error fetching account by ID: {e}")
        return None
    finally:
        conn.close()


def delete_account(account_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE account_id=?", (account_id,))
        conn.commit()
        changes = cursor.rowcount
        return changes > 0
    except Exception as e:
        print(f"Error deleting account: {e}")
        return False
    finally:
        conn.close()
