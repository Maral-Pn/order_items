import pyspark as ps
import pyspark.sql
from pyspark.sql import DataFrame

from core.config.pipeline_config import PipelineConfig
from core.io.data_source import DataSource
from core.io.dummy_data_source import DummyDataSource
from core.io.output_writer import OutputWriter
from core.schema.schema_repository import SchemaRepository
from core.schema.my_types import SchemaEnum
from core.transform.transformer import Transformer


class Pipeline:

    def __init__(self, config: PipelineConfig = None, data_source: DataSource = None,
                 output_writer: OutputWriter = None, verbose: bool = False):
        self.config = config or PipelineConfig()
        self.data_source = data_source
        self.output_writer = output_writer or OutputWriter()
        self.verbose = verbose
        self.schema_repository = SchemaRepository()
        self.spark = None
        self.transformer = None

    def initialiseSpark(self, master: str = None) -> pyspark.sql.SparkSession:
        master = master or self.config.master
        match master:
            case "local":
                spark = ps.sql.SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

            case m:
                spark = ps.sql.SparkSession.builder.appName("test").master(m).getOrCreate()

        self.spark = spark
        self.transformer = Transformer(spark)
        if self.data_source is None:
            self.data_source = DummyDataSource(spark)
        return spark

    def runPipeline(self):
        order_df = self.readOrder()
        product_df = self.readProduct()
        o_df = self.transformOrder(order_df)
        p_df = self.transformProduct(product_df)
        j_df_final = self.transformJoined(o_df, p_df)
        total_price = self.transformTotalPrice(j_df_final)
        self.write(total_price)

    def readProduct(self) -> DataFrame:
        return self.data_source.get_product_data()

    def readOrder(self) -> DataFrame:
        return self.data_source.get_order_data()

    def transformProduct(self, product_dataframe: DataFrame) -> DataFrame:
        products_schema = self.schema_repository.getSchema(SchemaEnum.PRODUCT)

        prd_rich_df = self.transformer.flattenProduct(product_dataframe, products_schema)
        prd_df = self.transformer.getProductDataframe(prd_rich_df)
        self._show(prd_df)
        return prd_df

    def transformOrder(self, order_dataframe: DataFrame) -> DataFrame:
        order_items_schema = self.schema_repository.getSchema(SchemaEnum.ORDER_ITEM)

        order_rich_df = self.transformer.flattenOrder(order_dataframe, order_items_schema)
        order_df = self.transformer.getOrderDataframe(order_rich_df)
        self._show(order_df)
        return order_df

    def transformJoined(self, order_df: DataFrame, prd_df: DataFrame) -> DataFrame:
        joined_df = self.transformer.joinDataframe(order_df, prd_df)
        self._show(joined_df)
        return joined_df

    def transformTotalPrice(self, joined_df: DataFrame) -> DataFrame:
        total_price_dataframe = self.transformer.getReceiptDataframe(joined_df, self.config.gst_rate)
        self._show(total_price_dataframe)
        return total_price_dataframe

    def write(self, dataframe: DataFrame):
        self.output_writer.write(
            dataframe,
            path=self.config.output_path,
            partition_column=self.config.partition_column,
            mode=self.config.write_mode,
        )
        self._show(dataframe)

    def _show(self, dataframe: DataFrame) -> None:
        if self.verbose:
            dataframe.show(truncate=False)
