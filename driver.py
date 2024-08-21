import pyspark as ps
from pyspark.context import SparkContext as sc
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from schema_repository import SchemaRepository
from my_types import SchemaEnum


if __name__ == "__main__":
    spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

    products = [
        ["""
    {
    "Id": 13,
    "Name": "Classic Desk",
    "Category": "Furniture",
    "Brand": "Ikea",
    "Color": "White",
    "Material": "MDF"
    }"""], ["""
    {
    "Id": 44,
    "Name": "Flower Chair",
    "Category": "Furniture",
    "Brand": "Ikea",
    "Color": "White",
    "Material": "cotton"
    }"""], ["""
    {
    "Id": 54,
    "Name": "Rectangular Dining table",
    "Category": "Furniture",
    "Brand": "Ikea",
    "Color": "Black",
    "Material": "MDF"
    }"""], ["""
    {
    "Id": 63,
    "Name": "Dining Chair",
    "Category": "Furniture",
    "Brand": "Ikea",
    "Color": "Blue",
    "Material": "MDF"
    }"""], ["""
    {
    "Id": 67,
    "Name": "Big Dish",
    "Category": "Kitchen",
    "Brand": "Ikea",
    "Color": "Brown",
    "Material": "Wood"
    }"""], ["""
    {
    "Id": 75,
    "Name": "Brown Flower",
    "Category": "Garden",
    "Brand": "Ikea",
    "Color": "Brown",
    "Material": "Plastic"
    }"""], ["""
    {
    "Id": 79,
    "Name": "Red Garden Flower Pot",
    "Category": "Garden",
    "Brand": "Ikea",
    "Color": "Red",
    "Material": "Plastic"
    }
    """]
    ]

    order_item_list = [

        """[{
    "product_id": 44,
    "unit_price": 550,
    "quantity": 6
    },
    {
    "product_id": 54,
    "unit_price": 1200,
    "quantity": 1
    },{
    "product_id": 67,
    "unit_price": 135,
    "quantity": 3
    },{
    "product_id": 75,
    "unit_price": 23,
    "quantity": 10
    },{
    "product_id": 79,
    "unit_price": 15,
    "quantity": 2
    }]""",

        """[{
    "product_id": 13,
    "unit_price": 620,
    "quantity": 2
    },{
    "product_id": 63,
    "unit_price": 188,
    "quantity": 8
    },{
    "product_id": 75,
    "unit_price": 23,
    "quantity": 1
    },{
    "product_id": 79,
    "unit_price": 15,
    "quantity": 1
    }]"""

    ]

    orders = [["Maral Pourdayan", "Parsa Pirouzfar", "2024-03-13", order_item_list[0]],
              ["Hossein Bakhtiari", "Shohre Tabatabae", "2024-04-05", order_item_list[1]]]

    # giving column names of dataframe
    columns = ["client_name", "seller_name", "purchase_date", "order_item_list"]

    # creating a dataframe
    orders_dataframe = spark.createDataFrame(orders, columns)

    # show data frame
    orders_dataframe.show(truncate=False)

    repo = SchemaRepository()
    order_items_schema = repo.getSchema(SchemaEnum.ORDER_ITEM)
    products_schema = repo.getSchema(SchemaEnum.PRODUCT)


    order_df = (orders_dataframe
                .withColumn("items", F.from_json(F.col("order_item_list"), order_items_schema))
                .withColumn("items_exp", F.explode(F.col("items")))
                .select(F.col("client_name"), F.col("seller_name"), F.col("purchase_date"), F.col("items_exp.*"))
                )
    order_df.show(truncate=False)

    columns = ["products"]
    product_df = spark.createDataFrame(products, columns)
    product_df.show(truncate=False)

    prd_df = (product_df
              .withColumn("PRD", F.from_json(F.col("products"), products_schema))
              .select(F.col("PRD.*"))
              )

    prd_df.show(truncate=False)

    joined_df = order_df.join(prd_df, order_df.product_id == prd_df.Id, how='inner')
    joined_df = joined_df.drop('Id')
    joined_df.show(truncate=False)

    joined_df.printSchema()
    gst = 1.03

    receipt_df = (joined_df
                  .groupBy(F.col("client_name"))
                  .agg(
                    F.sum(F.col("unit_price") * F.col("quantity") * gst)
                    .alias("Total_price_with_gst")
                       )
                  )
    receipt_df.show(truncate=False)

