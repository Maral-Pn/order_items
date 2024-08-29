from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType

class Transformer():
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def flattenOrder(self, dataframe: DataFrame, order_items_schema: StructType) -> DataFrame:
        df = (dataframe.withColumn("items", F.from_json(F.col("order_item_list"), order_items_schema))
              .withColumn("items_exp", F.explode(F.col("items")))
              )
        return df
    def flattenProduct(self, dataframe: DataFrame, products_schema: StructType) -> DataFrame:
        df = (dataframe
              .withColumn("PRD", F.from_json(F.col("products"), products_schema))
              )
        return df


    def getProductDataframe(self, dataframe: DataFrame) -> DataFrame:
        df = (dataframe.select(F.col("PRD.*")))
        return df

    def getOrderDataframe(self, dataframe: DataFrame) -> DataFrame:
        df = (dataframe
              .select(F.col("client_name"),
                      F.col("seller_name"),
                      F.col("purchase_date"),
                      F.col("items_exp.*")
                      )
              )
        return df
    def joinDataframe(self, order_df: DataFrame, prd_df: DataFrame):
        order_df.printSchema()
        joined_df = order_df.join(prd_df, order_df.product_id == prd_df.Id, how='inner')
        joined_df = joined_df.drop('Id')
        return joined_df

    def getReciptDataframe(self, df: DataFrame) -> DataFrame:
        gst = 1.03
        df = (df
              .groupBy(F.col("client_name"))
              .agg(F.sum(F.col("unit_price") * F.col("quantity") * gst)
                   .alias("Total_price_with_gst")))
        return df






