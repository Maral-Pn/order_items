from unittest import TestCase
from code.test.test_base import TestBase
from code.core.pipeline.my_pipeline import *


class TestTransformer(TestBase):
    def setUp(self):
        super().setUp()
        #self.transformer = Transformer(self.spark)
        self.pipeline = Pipeline()
        self.pipeline.initialiseSpark("local")

    def test_transformOrder(self):

        o_df = self.pipeline.readOrder()
        order_df = self.pipeline.transformOrder(o_df)

        orders = [
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 44, 550,6],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 54, 1200, 1],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 67, 135, 3],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 75, 23, 10],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 79, 15, 2],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 13, 620, 2],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 63, 188, 8],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 75, 23, 1],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 79, 15, 1]
                  ]

        columns = ["client_name", "seller_name", "purchase_date", "product_id", "unit_price", "quantity"]

        test_df = self.spark.createDataFrame(orders, columns)

        self.assertDataFrameEqual(order_df, test_df)


    def tearDown(self):
        self.pipeline.spark.stop()
        del self.pipeline
        super().tearDown()

if __name__ == '__main__':
    unittest.main()
