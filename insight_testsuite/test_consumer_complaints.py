from src.consumer_complaints import *
import unittest
from collections import Counter


class TestConsumerComplaints(unittest.TestCase):
    def test_csv_generator(self):
        actual = []
        for row in csv_generator('input/complaints.csv'):
            actual.append(row)

        expect = [['Date received', 'Product', 'Sub-product', 'Issue', 'Sub-issue', 'Consumer complaint narrative',
                   'Company public response', 'Company', 'State', 'ZIP code', 'Tags', 'Consumer consent provided?',
                   'Submitted via', 'Date sent to company', 'Company response to consumer', 'Timely response?',
                   'Consumer disputed?', 'Complaint ID'],
                  ['2019-09-24', 'Debt collection', 'I do not know', 'Attempts to collect debt not owed', 'Debt is not yours',
                   'transworld systems inc. is trying to collect a debt that is not mine, not owed and is inaccurate.', '',
                   'TRANSWORLD SYSTEMS INC', 'FL', '335XX', '', 'Consent provided', 'Web', '2019-09-24',
                   'Closed with explanation', 'Yes', 'N/A', '3384392'],
                  ['2019-09-19', 'Credit reporting, credit repair services, or other personal consumer reports',
                   'Credit reporting', 'Incorrect information on your report', 'Information belongs to someone else', '',
                   'Company has responded to the consumer and the CFPB and chooses not to provide a public response',
                   'Experian Information Solutions Inc.', 'PA', '15206', '', 'Consent not provided', 'Web', '2019-09-20',
                   'Closed with non-monetary relief', 'Yes', 'N/A', '3379500'],
                  ['2020-01-06', 'Credit reporting, credit repair services, or other personal consumer reports',
                   'Credit reporting', 'Incorrect information on your report', 'Information belongs to someone else', '', '',
                   'Experian Information Solutions Inc.', 'CA', '92532', '', 'N/A', 'Email', '2020-01-06', 'In progress', 'Yes',
                   'N/A', '3486776'],
                  ['2019-10-24', 'Credit reporting, credit repair services, or other personal consumer reports',
                   'Credit reporting', 'Incorrect information on your report', 'Information belongs to someone else', '',
                   'Company has responded to the consumer and the CFPB and chooses not to provide a public response',
                   'TRANSUNION INTERMEDIATE HOLDINGS, INC.', 'CA', '925XX', '', 'Other', 'Web', '2019-10-24',
                   'Closed with explanation', 'Yes', 'N/A', '3416481'],
                  ['2019-11-20', 'Credit reporting, credit repair services, or other personal consumer reports',
                   'Credit reporting', 'Incorrect information on your report', 'Account information incorrect',
                   'I would like the credit bureau to correct my XXXX XXXX XXXX XXXX balance. My correct balance is XXXX',
                   'Company has responded to the consumer and the CFPB and chooses not to provide a public response',
                   'TRANSUNION INTERMEDIATE HOLDINGS, INC.', 'TX', '77004', '', 'Consent provided', 'Web', '2019-11-20',
                   'Closed with explanation', 'Yes', 'N/A', '3444592']]

        self.assertEqual(actual, expect)

    def test_parse_csv(self):
        actual = parse_csv(csv_generator('input/complaints.csv'))
        expected = {'debt collection':
                        {2019: Counter({'transworld systems inc': 1})},
                    'credit reporting, credit repair services, or other personal consumer reports':
                        {2019: Counter({'transunion intermediate holdings, inc.': 2,
                                        'experian information solutions inc.': 1}),
                         2020: Counter({'experian information solutions inc.': 1})}}

        self.assertEqual(actual, expected)

    def test_get_header_pointers(self):
        actual_standard_headers = ['Date received', 'Product', 'Sub-product', 'Issue', 'Sub-issue', 'Consumer complaint narrative',
                                   'Company public response', 'Company', 'State', 'ZIP code', 'Tags', 'Consumer consent provided?',
                                   'Submitted via', 'Date sent to company', 'Company response to consumer', 'Timely response?',
                                   'Consumer disputed?', 'Complaint ID']
        self.assertEqual(get_header_pointers(actual_standard_headers), (0, 1, 7))

        basic_standard_headers = ['Company', 'Date received', 'Product']
        self.assertEqual(get_header_pointers(basic_standard_headers), (1, 2, 0))

        self.assertRaises(ValueError, get_header_pointers, [])
        self.assertRaises(ValueError, get_header_pointers, ['Company', 'Date received'])
        self.assertRaises(ValueError, get_header_pointers, ['a', 'b'])

    def test_extract_year(self):
        actual = extract_year("1998-01-01", 1)
        self.assertEqual(actual, 1998)
        self.assertRaises(ValueError, extract_year, '19980101', 1)

    def test_clean_str(self):
        self.assertEqual(clean_str("ABC "), "abc")
        self.assertEqual(clean_str("ab c"), "ab c")
        self.assertEqual(clean_str("aB c"), "ab c")

    def test_row_parsing(self):
        actual_result = {}
        row_parsing("product", 1998, "company", actual_result)
        self.assertEqual(actual_result, {'product': {1998: Counter({'company': 1})}})
        row_parsing("product", 1998, "company", actual_result)
        self.assertEqual(actual_result, {'product': {1998: Counter({'company': 2})}})
        row_parsing("product", 1999, "company", actual_result)
        self.assertEqual(actual_result, {'product': {1998: Counter({'company': 2}),
                                                     1999: Counter({'company': 1})}})
        row_parsing("product", 1999, "company2", actual_result)
        self.assertEqual(actual_result, {'product': {1998: Counter({'company': 2}),
                                                     1999: Counter({'company': 1,
                                                                    'company2': 1})}})
        row_parsing("product2", 1998, "company", actual_result)
        self.assertEqual(actual_result, {'product': {1998: Counter({'company': 2}),
                                                     1999: Counter({'company': 1, 'company2': 1})},
                                         'product2': {1998: Counter({'company': 1})}})

    def test_publish_results(self):
        results = {'product': {1998: Counter({'company': 2}),
                               1999: Counter({'company': 1, 'company2': 1})},
                   'product2': {1998: Counter({'company': 1})}}

        report_path = "output/actual_report.csv"
        publish_results(results, report_path)
        f_act = open(report_path)
        f_exp = open("output/expected_report.csv")
        actual = f_act.read()
        expect = f_exp.read()
        f_act.close()
        f_exp.close()
        self.assertEqual(actual, expect)

    def test_construct_row(self):
        results = {'product': {1998: Counter({'company': 2}),
                               1999: Counter({'company': 1, 'company2': 1})},
                   'product2': {1998: Counter({'company': 1})}}

        self.assertEqual(construct_row(results, 'product', 1998), ['product', '1998', '2', '1', '100'])
        self.assertEqual(construct_row(results, 'product', 1999), ['product', '1999', '2', '2', '50'])

    def test_total_complaints(self):
        self.assertEqual(total_complaints(Counter({'company': 10})), 10)
        self.assertEqual(total_complaints(Counter({'company': 10, 'company2': 5})), 15)

    def test_total_companies(self):
        self.assertEqual(total_companies(Counter({'company': 10})), 1)
        self.assertEqual(total_companies(Counter({'company': 10, 'company2': 5})), 2)

    def test_highest_percentage(self):
        self.assertEqual(highest_percentage(Counter({'company': 2}), 2), 100)
        self.assertEqual(highest_percentage(Counter({'company': 2, 'a': 1}), 3), 67)
        self.assertEqual(highest_percentage(Counter({'company': 1, 'a': 1}), 2), 50)
        self.assertEqual(highest_percentage(Counter({'company': 999, 'a': 1}), 1000), 100)
        self.assertEqual(highest_percentage(Counter({'company': 1, 'a': 1, 'b': 1}), 3), 33)


if __name__ == '__main__':
    unittest.main()
