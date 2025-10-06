import unittest
from services import loan_service

class TestLoanEligibility(unittest.TestCase):

    def test_saving_with_pan(self):
        account = {
            'account_type': 'saving',
            'pan_number': 'ABCDE1234F'
        }
        loans = loan_service.check_loan_eligibility(account)
        self.assertIn('home loan', loans)
        self.assertIn('business loan', loans, msg="Business loan shouldn't be eligible")
        self.assertNotIn('business loan', loans)

    def test_current_with_tan(self):
        account = {
            'account_type': 'current',
            'tan_number': 'TAN1234567'
        }
        loans = loan_service.check_loan_eligibility(account)
        self.assertIn('business loan', loans)
        self.assertNotIn('home loan', loans)

    def test_no_pan_or_tan(self):
        account = {
            'account_type': 'saving',
            'pan_number': None,
            'tan_number': None
        }
        loans = loan_service.check_loan_eligibility(account)
        self.assertEqual(loans, [])

if __name__ == "__main__":
    unittest.main()
