import pandas as pd
import sqlite3
from flask import Flask, jsonify, request

app = Flask(__name__)

class ETLError(Exception):
    pass


def categorize_balance(bal):
    if bal < 500:
        return 'low'
    elif 500 <= bal <= 2000:
        return 'medium'
    else:
        return 'high'

def run_etl(csv_file, db_file="bank.db"):
    try:
        # Extract
        df = pd.read_csv(csv_file)
        if df.empty:
            raise ETLError("CSV file is empty!")

        # Transform
        df['balance'] = df['balance'].apply(lambda x: float(x) if x >= 0 else 0)
        df['account_name'] = df['account_name'].fillna("Unknown")
        df['account_name'] = df['account_name'].apply(lambda x: x.title())
        df['balance_category'] = df['balance'].apply(categorize_balance)
        df = df.drop_duplicates(subset=['account_id'])

        # Load
        conn = sqlite3.connect(db_file)
        df.to_sql("accounts", conn, if_exists="replace", index=False)
        conn.close()

        print("ETL Completed Successfully!")
    except FileNotFoundError:
        print(f"Error: File {csv_file} not found.")
    except ETLError as e:
        print(f"ETL Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

def read_from_db(db_file="bank.db", table_name="accounts"):
    try:
        conn = sqlite3.connect(db_file)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df

    except Exception as e:
        print(f"Error reading from database: {e}")
        return None


def insert_into_db(record, db_file="bank.db", table_name="accounts"):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute(
            f"""
            INSERT INTO {table_name} (account_id, account_name, balance,balance_category)
            VALUES (?, ?, ?, ?)
            """,
            (record['account_id'], record['account_name'], record['balance'],record['balance_category'])
        )

        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        return False
    except Exception as e:
        print(f"Error inserting into database: {e}")
        return False


def delete_from_db(account_id, db_file="bank.db", table_name="accounts"):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute(
            f"DELETE FROM {table_name} WHERE account_id = ?",
            (account_id,)
        )

        conn.commit()
        changes = cursor.rowcount  # Number of rows affected
        conn.close()

        return changes > 0  # Return True if a row was deleted

    except Exception as e:
        print(f"Error deleting from database: {e}")
        return False



@app.route('/accounts', methods=['GET'])
def get_accounts():
    df = read_from_db()
    if df is not None:
        data = df.to_dict(orient='records')
        return jsonify(data), 200
    else:
        return jsonify({"error": "Failed to fetch data"}), 500


@app.route('/accounts', methods=['POST'])
def add_account():
    new_data = request.get_json()

    required_fields = ['account_id', 'account_name', 'balance']
    if not all(field in new_data for field in required_fields):
        return jsonify({"error": f"Missing one of required fields: {required_fields}"}), 400

    try:
        balance = float(new_data['balance'])
        if balance < 0:
            balance = 0
    except ValueError:
        return jsonify({"error": "Balance must be a number"}), 400

    account_name = new_data['account_name'].title()
    balance_category = categorize_balance(balance)

    record = {
        'account_id': new_data['account_id'],
        'account_name': account_name,
        'balance': balance,
        'balance_category':balance_category
    }

    df = read_from_db()
    if df is not None and record['account_id'] in df['account_id'].values:
        return jsonify({"error": "account_id already exists"}), 409

    success = insert_into_db(record)
    if success:
        return jsonify({"message": "Account added successfully"}), 201
    else:
        return jsonify({"error": "Failed to insert account"}), 500

@app.route('/accounts/<int:account_id>', methods=['DELETE'])
def delete_account(account_id):
    success = delete_from_db(account_id)
    if success:
        return jsonify({"message": f"Account with id {account_id} deleted successfully"}), 200
    else:
        return jsonify({"error": f"No account found with id {account_id}"}), 404

if __name__ == "__main__":
    app.run(debug=True)
