import unittest
from google.cloud import bigquery
from bigquery_handling import get_results

PROJECT = 'peaceful-tide-284813'

class TestQuery(unittest.TestCase):

    def setUp(self):
        self.bq_client = bigquery.Client(project = PROJECT)

    def tearDown(self):
        pass

    def test_qrcode_case(self):
        self.assertTrue(get_results('C0SFT7', self.bq_client).startswith("Gross amount: 3.16, Ride distance: 2198.9"))
    
    def test_vid_case(self):
        self.assertTrue(get_results('00TW9JELAItFazRxej7f', self.bq_client).startswith("Gross amount: 3.16, Ride distance: 2198.9"))

    def test_bad_case(self):
        self.assertEqual(get_results('blahblah', self.bq_client), "This query didn't hit any record.")

    def test_wrong_case(self):
        with self.assertRaises(TypeError):
            get_results(0, self.bq_client)


if __name__ == '__main__':
    unittest.main()