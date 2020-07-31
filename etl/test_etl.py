import unittest
from main import etl_core

class TestQuery(unittest.TestCase):

    def test_dup_case(self):
        self.assertEqual(etl_core("dott_test"), "dummy")
    
    def test_nodup_case(self):
        self.assertEqual(etl_core("dott_test_nodup"), "dummy")

if __name__ == '__main__':
    unittest.main()