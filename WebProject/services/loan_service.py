import pandas as pd
from models import account_model


class ETLError(Exception):
    pass


def run_etl(csv_file):
    df = pd.read_csv(csv_file)
    if df.empty:
        raise ETLError("CSV file is empty!")

    # Basic cleaning
    df['account_name'] = df['account_name'].fillna("Unknown").apply(lambda x: x.title())
    df['account_type'] = df['account_type'].str.lower()
    df['balance'] = df['balance'].apply(lambda x: float(x) if x >= 0 else 0)

    # Deduplicate
    df = df.drop_duplicates(subset=['account_id'])

    # Insert into DB
    account_model.create_table()
    for _, row in df.iterrows():
        account = {
            'account_id': row['account_id'],
            'account_name': row['account_name'],
            'account_type': row['account_type'],
            'balance': row['balance'],
            'pan_number': row.get('pan_number', None),
            'tan_number': row.get('tan_number', None)
        }
        account_model.insert_account(account)
    return True


def check_loan_eligibility(account):
    """Return list of loans eligible for the account."""
    loans = []
    if account['account_type'] == 'saving' and account.get('pan_number'):
        loans = ['home loan', 'car loan', 'education loan', 'personal loan']
    elif account['account_type'] == 'current' and account.get('tan_number'):
        loans = ['business loan']
    return loans
