from model import BankAccount

# --------------------
# Controller
# --------------------
class BankController:
    def __init__(self, view):
        self.view = view
        self.accounts = {}
        self.next_account_number = 1001

    def create_account(self, holder_name, initial_balance=0):
        acc_num = self.next_account_number
        account = BankAccount(acc_num, holder_name, initial_balance)
        self.accounts[acc_num] = account
        self.next_account_number += 1
        self.view.show_message(f"Account created successfully! Account Number: {acc_num}")

    def deposit(self, account_number, amount):
        account = self.accounts.get(account_number)
        if account:
            account.deposit(amount)
            self.view.show_message(f"Deposited ₹{amount} to Account {account_number}")
        else:
            self.view.show_message("Account not found!")

    def withdraw(self, account_number, amount):
        account = self.accounts.get(account_number)
        if account:
            if account.withdraw(amount):
                self.view.show_message(f"Withdrew ₹{amount} from Account {account_number}")
            else:
                self.view.show_message("Insufficient funds!")
        else:
            self.view.show_message("Account not found!")

    def show_account(self, account_number):
        account = self.accounts.get(account_number)
        if account:
            self.view.show_account_details(account)
        else:
            self.view.show_message("Account not found!")
