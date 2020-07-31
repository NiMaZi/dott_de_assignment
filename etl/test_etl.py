import unittest
from .main import etl_core

class TestQuery(unittest.TestCase):

    def test_dup_case(self):
        self.assertEqual(etl_core("dott_test"), "115103 records loaded, 151283 duplicated records discarded.")
    
    def test_nodup_case(self):
        self.assertEqual(etl_core("dott_test_nodup"), "115103 records loaded, 0 duplicated records discarded.")

if __name__ == '__main__':
    unittest.main()