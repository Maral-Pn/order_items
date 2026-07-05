from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class DataSource(ABC):
    """Where the pipeline gets its raw order/product data from.

    Swap in a different implementation (e.g. reading Parquet/JDBC) without
    touching Pipeline or Transformer.
    """

    @abstractmethod
    def get_order_data(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def get_product_data(self) -> DataFrame:
        raise NotImplementedError
