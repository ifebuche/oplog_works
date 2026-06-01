import unittest

from MI_ETL.core.helpers import data_volume, number_to_decimal


class TestHelpers(unittest.TestCase):
    def test_data_volume(self):
        size, mb = data_volume([{"a": 1}, {"b": 2}])
        self.assertGreater(size, 0)
        self.assertGreater(mb, 0)

    def test_number_to_decimal(self):
        ok, data = number_to_decimal({"count": 10, "rate": 1.5})
        self.assertTrue(ok)
        from decimal import Decimal

        self.assertIsInstance(data["count"], Decimal)


if __name__ == "__main__":
    unittest.main()
