from controller import BankController
from view import BankView

if __name__ == "__main__":
    view = BankView()
    controller = BankController(view)

    # Create accounts
    controller.create_account("Alice", 1000)
    controller.create_account("Bob", 500)

    # Operations
    controller.deposit(1001, 200)
    controller.withdraw(1001, 150)
    controller.show_account(1001)

    controller.deposit(1002, 1000)
    controller.withdraw(1002, 2000)  # Insufficient funds
    controller.show_account(1002)
