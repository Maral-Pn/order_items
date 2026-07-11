import os
import tempfile
import unittest

from test.test_base import TestBase
from core.pipeline.my_pipeline import *
from core.config.pipeline_config import PipelineConfig


class TestPipeline(TestBase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline()
        self.pipeline.initialiseSpark("local")

    def test_transformOrder(self):

        o_df = self.pipeline.readOrder()
        order_df = self.pipeline.transformOrder(o_df)

        orders = [
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 44, 550.0, 6],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 54, 1200.0, 1],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 67, 135.0, 3],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 75, 23.0, 10],
                  ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 79, 15.0, 2],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 13, 620.0, 2],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 63, 188.0, 8],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 75, 23.0, 1],
                  ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", 79, 15.0, 1]
                  ]

        columns = ["client_name", "seller_name", "purchase_date", "product_id", "unit_price", "quantity"]

        test_df = self.spark.createDataFrame(orders, columns)

        self.assertDataFrameEqual(order_df, test_df)

    def test_transformProduct(self):
        p_df = self.pipeline.readProduct()
        product_df = self.pipeline.transformProduct(p_df)

        products = [
            [13, "Classic Desk", "Furniture", "Ikea", "White", "MDF"],
            [44, "Flower Chair", "Furniture", "Ikea", "White", "cotton"],
            [54, "Rectangular Dining table", "Furniture", "Ikea", "Black", "MDF"],
            [63, "Dining Chair", "Furniture", "Ikea", "Blue", "MDF"],
            [67, "Big Dish", "Kitchen", "Ikea", "Brown", "Wood"],
            [75, "Brown Flower", "Garden", "Ikea", "Brown", "Plastic"],
            [79, "Red Garden Flower Pot", "Garden", "Ikea", "Red", "Plastic"],
        ]
        columns = ["Id", "Name", "Category", "Brand", "Color", "Material"]
        test_df = self.spark.createDataFrame(products, columns)

        self.assertDataFrameEqual(
            product_df.orderBy("Id"),
            test_df.orderBy("Id"),
        )

    def test_transformJoined(self):
        o_df = self.pipeline.transformOrder(self.pipeline.readOrder())
        p_df = self.pipeline.transformProduct(self.pipeline.readProduct())

        joined_df = self.pipeline.transformJoined(o_df, p_df)

        self.assertEqual(joined_df.count(), 9)
        self.assertNotIn("Id", joined_df.columns)
        self.assertIn("Name", joined_df.columns)

        flower_chair_row = joined_df.filter(joined_df.product_id == 44).collect()[0]
        self.assertEqual(flower_chair_row["Name"], "Flower Chair")
        self.assertEqual(flower_chair_row["unit_price"], 550.0)
        self.assertEqual(flower_chair_row["quantity"], 6)

    def test_transformTotalPrice(self):
        o_df = self.pipeline.transformOrder(self.pipeline.readOrder())
        p_df = self.pipeline.transformProduct(self.pipeline.readProduct())
        joined_df = self.pipeline.transformJoined(o_df, p_df)

        total_price_df = self.pipeline.transformTotalPrice(joined_df)

        gst_rate = self.pipeline.config.gst_rate
        maral_total = (550 * 6 + 1200 * 1 + 135 * 3 + 23 * 10 + 15 * 2) * gst_rate
        hossein_total = (620 * 2 + 188 * 8 + 23 * 1 + 15 * 1) * gst_rate

        expected_rows = [
            ["Maral Pourdayan", maral_total],
            ["Hossein Bakhtiari", hossein_total],
        ]
        expected_columns = ["client_name", "Total_price_with_gst"]
        expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

        self.assertDataFrameEqual(
            total_price_df.orderBy("client_name"),
            expected_df.orderBy("client_name"),
        )

    def test_write(self):
        df = self.spark.createDataFrame(
            [["Maral Pourdayan", 100.0]], ["client_name", "Total_price_with_gst"]
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_path = os.path.join(tmp, "out")
            config = PipelineConfig(
                output_path=output_path,
                partition_column="client_name",
                write_mode="overwrite",
            )
            pipeline = Pipeline(config=config)
            pipeline.initialiseSpark("local")
            try:
                pipeline.write(df)

                read_back = self.spark.read.json(output_path)
                self.assertDataFrameEqual(
                    read_back.select("client_name", "Total_price_with_gst"),
                    df,
                )
            finally:
                del pipeline

    def test_runPipeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_path = os.path.join(tmp, "out")
            config = PipelineConfig(
                output_path=output_path,
                partition_column="client_name",
                write_mode="overwrite",
            )
            pipeline = Pipeline(config=config)
            pipeline.initialiseSpark("local")
            try:
                pipeline.runPipeline()

                gst_rate = config.gst_rate
                maral_total = (550 * 6 + 1200 * 1 + 135 * 3 + 23 * 10 + 15 * 2) * gst_rate
                hossein_total = (620 * 2 + 188 * 8 + 23 * 1 + 15 * 1) * gst_rate

                expected_rows = [
                    ["Maral Pourdayan", maral_total],
                    ["Hossein Bakhtiari", hossein_total],
                ]
                expected_columns = ["client_name", "Total_price_with_gst"]
                expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

                read_back = self.spark.read.json(output_path)
                self.assertDataFrameEqual(
                    read_back.select("client_name", "Total_price_with_gst").orderBy("client_name"),
                    expected_df.orderBy("client_name"),
                )
            finally:
                del pipeline

    def test_readOrder(self):
        self.assertDataFrameEqual(
            self.pipeline.readOrder(),
            self.pipeline.data_source.get_order_data(),
        )

    def test_readProduct(self):
        self.assertDataFrameEqual(
            self.pipeline.readProduct(),
            self.pipeline.data_source.get_product_data(),
        )

    def test_initialiseSpark_nonLocalMaster(self):
        spark = self.pipeline.initialiseSpark("local[2]")

        self.assertIsNotNone(spark)
        self.assertIsNotNone(self.pipeline.transformer)
        self.assertIsNotNone(self.pipeline.data_source)

    def tearDown(self):
        self.pipeline.spark.stop()
        del self.pipeline
        super().tearDown()

if __name__ == '__main__':
    unittest.main()
