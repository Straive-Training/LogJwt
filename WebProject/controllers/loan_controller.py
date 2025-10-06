from flask import Blueprint, request, jsonify
from models import account_model
from services import loan_service

loan_bp = Blueprint('loan_bp', __name__)


@loan_bp.route('/accounts', methods=['GET'])
def get_accounts():
    df = account_model.get_all_accounts()
    data = df.to_dict(orient='records')
    return jsonify(data), 200

@loan_bp.route('/accounts', methods=['POST'])
def add_account():
    new_data = request.get_json()

    required_fields = ['account_id', 'account_name', 'account_type', 'balance']
    # pan_number and tan_number are optional but accepted
    if not all(field in new_data for field in required_fields):
        return jsonify({"error": f"Missing required fields: {required_fields}"}), 400

    try:
        balance = float(new_data['balance'])
        if balance < 0:
            balance = 0
    except (ValueError, TypeError):
        return jsonify({"error": "Balance must be a number"}), 400

    account_name = new_data['account_name'].title()
    account_type = new_data['account_type'].lower()
    if account_type not in ['saving', 'current']:
        return jsonify({"error": "account_type must be either 'saving' or 'current'"}), 400

    record = {
        'account_id': new_data['account_id'],
        'account_name': account_name,
        'account_type': account_type,
        'balance': balance,
        'pan_number': new_data.get('pan_number'),
        'tan_number': new_data.get('tan_number')
    }

    df = account_model.get_all_accounts()
    if record['account_id'] in df['account_id'].values:
        return jsonify({"error": "account_id already exists"}), 409

    success = account_model.insert_account(record)
    if success:
        return jsonify({"message": "Account added successfully"}), 201
    else:
        return jsonify({"error": "Failed to insert account"}), 500


@loan_bp.route('/accounts/<int:account_id>/loans', methods=['GET'])
def get_loan_eligibility(account_id):
    pan = request.args.get('pan_number')
    tan = request.args.get('tan_number')

    # Fetch account from DB
    row = account_model.get_account_by_id(account_id)
    if not row:
        return jsonify({"error": "Account not found"}), 404

    keys = ['account_id', 'account_name', 'account_type', 'balance', 'pan_number', 'tan_number']
    account = dict(zip(keys, row))

    # Validate PAN/TAN
    if account['account_type'] == 'saving':
        if not pan:
            return jsonify({"error": "PAN number is required for saving accounts"}), 400
        if pan != account.get('pan_number'):
            return jsonify({"error": "PAN number does not match"}), 403
    elif account['account_type'] == 'current':
        if not tan:
            return jsonify({"error": "TAN number is required for current accounts"}), 400
        if tan != account.get('tan_number'):
            return jsonify({"error": "TAN number does not match"}), 403
    else:
        return jsonify({"error": "Invalid account type"}), 400

    loans = loan_service.check_loan_eligibility(account)
    return jsonify({"eligible_loans": loans}), 200



@loan_bp.route('/accounts/<int:account_id>', methods=['DELETE'])
def delete_account(account_id):
    success = account_model.delete_account(account_id)
    if success:
        return jsonify({"message": "Account deleted"}), 200
    else:
        return jsonify({"error": "Account not found"}), 404
