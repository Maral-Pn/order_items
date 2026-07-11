import unittest
from test.test_base import TestBase
from core.transform.transformer import Transformer
from core.schema.schema_repository import SchemaRepository
from core.schema.my_types import SchemaEnum


class TestTransformer(TestBase):
    def setUp(self):
        super().setUp()
        self.transformer = Transformer(self.spark)
        self.schema_repository = SchemaRepository()

    def test_flattenOrder(self):
        orders = [["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13",
                    '[{"product_id": 44, "unit_price": 550, "quantity": 6},'
                    '{"product_id": 54, "unit_price": 1200, "quantity": 1}]']]
        columns = ["client_name", "seller_name", "purchase_date", "order_item_list"]
        order_df = self.spark.createDataFrame(orders, columns)

        schema = self.schema_repository.getSchema(SchemaEnum.ORDER_ITEM)
        result = self.transformer.flattenOrder(order_df, schema)
        flattened = result.select("client_name", "seller_name", "purchase_date", "items_exp.*")

        expected_rows = [
            ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 44, 550.0, 6],
            ["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", 54, 1200.0, 1],
        ]
        expected_columns = ["client_name", "seller_name", "purchase_date", "product_id", "unit_price", "quantity"]
        expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

        self.assertDataFrameEqual(flattened, expected_df)

    def test_flattenProduct(self):
        products = [['{"Id": 44, "Name": "Flower Chair", "Category": "Furniture", '
                     '"Brand": "Ikea", "Color": "White", "Material": "cotton"}']]
        columns = ["products"]
        product_df = self.spark.createDataFrame(products, columns)

        schema = self.schema_repository.getSchema(SchemaEnum.PRODUCT)
        result = self.transformer.flattenProduct(product_df, schema)
        flattened = result.select("PRD.*")

        expected_rows = [[44, "Flower Chair", "Furniture", "Ikea", "White", "cotton"]]
        expected_columns = ["Id", "Name", "Category", "Brand", "Color", "Material"]
        expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

        self.assertDataFrameEqual(flattened, expected_df)

    def test_getProductDataframe(self):
        products = [['{"Id": 44, "Name": "Flower Chair", "Category": "Furniture", '
                     '"Brand": "Ikea", "Color": "White", "Material": "cotton"}']]
        columns = ["products"]
        product_df = self.spark.createDataFrame(products, columns)

        schema = self.schema_repository.getSchema(SchemaEnum.PRODUCT)
        flattened = self.transformer.flattenProduct(product_df, schema)
        result = self.transformer.getProductDataframe(flattened)

        expected_rows = [[44, "Flower Chair", "Furniture", "Ikea", "White", "cotton"]]
        expected_columns = ["Id", "Name", "Category", "Brand", "Color", "Material"]
        expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

        self.assertDataFrameEqual(result, expected_df)

    def test_joinDataframe(self):
        order_rows = [
            ["Maral Pourdayan", 44, 550, 6],
            ["Maral Pourdayan", 999, 10, 1],
        ]
        order_df = self.spark.createDataFrame(
            order_rows, ["client_name", "product_id", "unit_price", "quantity"]
        )

        product_rows = [[44, "Flower Chair"]]
        product_df = self.spark.createDataFrame(product_rows, ["Id", "Name"])

        joined = self.transformer.joinDataframe(order_df, product_df)

        expected_rows = [["Maral Pourdayan", 44, 550, 6, "Flower Chair"]]
        expected_columns = ["client_name", "product_id", "unit_price", "quantity", "Name"]
        expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

        self.assertDataFrameEqual(joined, expected_df)

    def test_getReceiptDataframe(self):
        rows = [
            ["Maral Pourdayan", 550, 6],
            ["Maral Pourdayan", 1200, 1],
            ["Hossein Bakhtiari", 620, 2],
        ]
        df = self.spark.createDataFrame(rows, ["client_name", "unit_price", "quantity"])

        result = self.transformer.getReceiptDataframe(df, gst_rate=1.1)

        expected_rows = [
            ["Maral Pourdayan", (550 * 6 + 1200 * 1) * 1.1],
            ["Hossein Bakhtiari", (620 * 2) * 1.1],
        ]
        expected_columns = ["client_name", "Total_price_with_gst"]
        expected_df = self.spark.createDataFrame(expected_rows, expected_columns)

        self.assertDataFrameEqual(result, expected_df)


if __name__ == '__main__':
    unittest.main()
