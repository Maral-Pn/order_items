from pyspark.sql import SparkSession, DataFrame

class InputOutput:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def getDummyOrder(self) -> DataFrame:


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

        columns = ["client_name", "seller_name", "purchase_date", "order_item_list"]
        order_dataframe = self.spark.createDataFrame(orders, columns)

        return order_dataframe
    def getDummyProduct(self) -> DataFrame:
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

        columns = ["products"]
        product_dataframe = self.spark.createDataFrame(products, columns)
        return product_dataframe



    def getParquet(self, spark: SparkSession):
        pass