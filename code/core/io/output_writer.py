from pyspark.sql import DataFrame


class OutputWriter:
    """Writes a DataFrame to a sink, decoupling Pipeline from a specific output target/format."""

    def write(self, dataframe: DataFrame, path: str, partition_column: str = None,
              mode: str = "overwrite", fmt: str = "json") -> None:
        writer = dataframe.write.mode(mode)
        if partition_column:
            writer = writer.partitionBy(partition_column)
        getattr(writer, fmt)(path)
