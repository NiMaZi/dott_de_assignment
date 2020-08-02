import unittest
from .main import dataflow_etl_core

class TestQuery(unittest.TestCase):

    def test_dup_case(self):
        self.assertEqual(dataflow_etl_core("dott_de_assignment_with_dups"), "115103 records loaded, 151283 duplicated records discarded.")
    
    def test_nodup_case(self):
        self.assertEqual(dataflow_etl_core("dott_de_assignment_bucket"), "115103 records loaded, 0 duplicated records discarded.")

if __name__ == '__main__':
    unittest.main()