# --------------------
# View
# --------------------
class BankView:
    def show_message(self, message):
        print(message)

    def show_account_details(self, account):
        print(f"Account Number: {account.account_number}, "
              f"Holder: {account.account_holder}, Balance: ₹{account.balance}")
