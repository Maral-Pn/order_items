from unittest import TestCase
from pyspark.sql import SparkSession, DataFrame
import pyspark.testing as ts

class TestBase(TestCase):
    def setUp(self):
        self.spark = (
            SparkSession.builder.master("local[*]").appName("Tester").getOrCreate()
        )

    def assertDataFrameEqual(self, dfA : DataFrame , dfB : DataFrame) -> None:
        ts.assertSchemaEqual(dfA.schema, dfB.schema)
        ts.assertDataFrameEqual(dfA, dfB)



    def tearDown(self):
        self.spark.stop()

if __name__ == "__main__":
    unittest.main()