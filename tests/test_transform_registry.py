import unittest

from MI_ETL.transforms.registry import TransformRegistry


class TestTransformRegistry(unittest.TestCase):
    def test_register_and_transform(self):
        reg = TransformRegistry()

        def handler(data):
            return True, [{"table_name": "out", "table": data}]

        reg.register("src", handler)
        result = reg.transform("src", [{"a": 1}])
        self.assertTrue(result.ok)
        self.assertEqual(len(result.load_targets), 1)

    def test_missing_handler(self):
        reg = TransformRegistry()
        result = reg.transform("missing", [])
        self.assertFalse(result.ok)


if __name__ == "__main__":
    unittest.main()
