
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, ArrayType
from core.schema.my_types import SchemaEnum

class SchemaRepository:
    def __init__(self):
        pass
    def getSchema(self, type: SchemaEnum) -> StructType:
        match type:
            case SchemaEnum.PRODUCT:
                schema = StructType([
                    StructField("Id", LongType(), True),
                    StructField("Name", StringType(), True),
                    StructField("Category", StringType(), True),
                    StructField("Brand", StringType(), True),
                    StructField("Color", StringType(), True),
                    StructField("Material", StringType(), True)
                ])
            case SchemaEnum.ORDER_ITEM:
                inner_order_items_schema = StructType([
                    StructField("product_id", LongType(), True),
                    StructField("unit_price", LongType(), True),
                    StructField("quantity", LongType(), True),
                ])

                schema = ArrayType(inner_order_items_schema, True)

            case _:
                raise NotImplemented

        return schema