import pyspark as ps
import pyspark.sql
from pyspark.sql import DataFrame
from core.schema.schema_repository import SchemaRepository
from core.schema.my_types import SchemaEnum
from core.io.input_output import InputOutput
from core.transform.transformer import Transformer


class Pipeline:

    def initialiseSpark(self, master: str) -> pyspark.sql.SparkSession:
        match master:
            case "local":
                spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

            case m:
                spark = ps.sql.SparkSession.builder.appName("test").master(m).getOrCreate()
        self.spark = spark


    def runPipeline(self):
        order_df = self.readOrder()
        product_df = self.readProduct()
        o_df = self.transformOrder(order_df)
        o_df.show(truncate=False)
        p_df = self.transformProduct(product_df)
        j_df_final = self.transformJoined(o_df, p_df)
        total_price = self.transformTotalPrice(j_df_final)
        self.write(total_price)


    def readProduct(self):
        product_dataframe = InputOutput(self.spark).getDummyProduct()
        return product_dataframe

    def readOrder(self):
        order_dataframe = InputOutput(self.spark).getDummyOrder()
        return order_dataframe

    def transformProduct(self, product_dataframe: DataFrame) -> DataFrame:

        repo = SchemaRepository()
        products_schema = repo.getSchema(SchemaEnum.PRODUCT)

        transformer = Transformer(self.spark)
        prd_rich_df = transformer.flattenProduct(product_dataframe, products_schema)
        prd_rich_df.show(truncate=False)
        prd_df = transformer.getProductDataframe(prd_rich_df)
        prd_df.show(truncate=False)
        return prd_df

    def transformOrder(self, order_dataframe: DataFrame) -> DataFrame:
        repo = SchemaRepository()
        order_items_schema = repo.getSchema(SchemaEnum.ORDER_ITEM)

        transformer = Transformer(self.spark)
        order_rich_df = transformer.flattenOrder(order_dataframe, order_items_schema)
        order_df = transformer.getOrderDataframe(order_rich_df)
        return order_df

    def transformJoined(self, order_df: DataFrame, prd_df: DataFrame) -> DataFrame:
        transformer = Transformer(self.spark)
        joined_df = transformer.joinDataframe(order_df, prd_df)
        joined_df.show(truncate=False)
        return joined_df

    def transformTotalPrice(self, joined_df) -> DataFrame:
        transformer = Transformer(self.spark)
        total_price_dataframe = transformer.getReciptDataframe(joined_df)
        total_price_dataframe.show(truncate=False)
        return total_price_dataframe

    def write(self, dataframe: DataFrame):
        dataframe.write.mode("overwrite").partitionBy("client_name").json("total_price")
        dataframe.show(truncate=False)
















