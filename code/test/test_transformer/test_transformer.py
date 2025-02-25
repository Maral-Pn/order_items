from unittest import TestCase
from code.test.test_base import TestBase
from code.core.transform.transformer import Transformer


class TestTransformer(TestBase):
    def setUp(self):
        super().setUp()
        self.transformer = Transformer(self.spark)

    def test_flattenOrder(self):
        pass

    def test_flattenProduct(self):
        pass


if __name__ == '__main__':
    unittest.main()

