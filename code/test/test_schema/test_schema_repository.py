import unittest

from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    LongType,
    DoubleType,
    StringType,
)

from core.schema.schema_repository import SchemaRepository
from core.schema.my_types import SchemaEnum


class TestSchemaRepository(unittest.TestCase):
    def setUp(self):
        self.repository = SchemaRepository()

    def test_product_schema(self):
        schema = self.repository.getSchema(SchemaEnum.PRODUCT)

        expected = StructType([
            StructField("Id", LongType(), True),
            StructField("Name", StringType(), True),
            StructField("Category", StringType(), True),
            StructField("Brand", StringType(), True),
            StructField("Color", StringType(), True),
            StructField("Material", StringType(), True),
        ])
        self.assertEqual(schema, expected)

    def test_order_item_schema(self):
        schema = self.repository.getSchema(SchemaEnum.ORDER_ITEM)

        expected = ArrayType(
            StructType([
                StructField("product_id", LongType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("quantity", LongType(), True),
            ]),
            True,
        )
        self.assertEqual(schema, expected)

    def test_unregistered_schema_raises(self):
        with self.assertRaises(NotImplementedError):
            self.repository.getSchema(None)


if __name__ == "__main__":
    unittest.main()
