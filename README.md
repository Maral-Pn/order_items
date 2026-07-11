# order_items

A local PySpark batch job that joins order items with product data and computes each
client's total price including GST.

## Architecture

`Pipeline` (`code/core/pipeline/my_pipeline.py`) orchestrates the job but doesn't own any
of the actual logic — everything it needs is injected:

- **`DataSource`** (`code/core/io/data_source.py`) — abstract source of order/product
  data. `DummyDataSource` is the only implementation today (in-memory fixture data); swap
  in a new `DataSource` to read from Parquet/JDBC/etc. without touching `Pipeline`.
- **`SchemaRepository`** (`code/core/schema/schema_repository.py`) — the source of truth
  for the `StructType`/`ArrayType` schemas used to parse the JSON-string columns coming
  out of the data source.
- **`Transformer`** (`code/core/transform/transformer.py`) — the actual Spark
  transformations (flatten, join, aggregate). Pure: no I/O, no printing.
- **`OutputWriter`** (`code/core/io/output_writer.py`) — writes the result DataFrame to a
  sink; format/path/partitioning are all parameters, not hardcoded.
- **`PipelineConfig`** (`code/core/config/pipeline_config.py`) — the one place that holds
  environment-specific knobs (Spark master, output path/partitioning, GST rate).

### UML

```mermaid
classDiagram
    class Pipeline {
        -PipelineConfig config
        -DataSource data_source
        -OutputWriter output_writer
        -SchemaRepository schema_repository
        -Transformer transformer
        -bool verbose
        +initialiseSpark(master) SparkSession
        +runPipeline()
        +readOrder() DataFrame
        +readProduct() DataFrame
        +transformOrder(df) DataFrame
        +transformProduct(df) DataFrame
        +transformJoined(order_df, prd_df) DataFrame
        +transformTotalPrice(df) DataFrame
        +write(df)
    }

    class PipelineConfig {
        <<dataclass>>
        +str master
        +str output_path
        +str partition_column
        +str write_mode
        +float gst_rate
    }

    class DataSource {
        <<abstract>>
        +get_order_data() DataFrame
        +get_product_data() DataFrame
    }

    class DummyDataSource {
        -SparkSession spark
        +get_order_data() DataFrame
        +get_product_data() DataFrame
    }

    class OutputWriter {
        +write(df, path, partition_column, mode, fmt)
    }

    class SchemaRepository {
        +getSchema(schema_type) StructType
    }

    class SchemaEnum {
        <<enumeration>>
        PRODUCT
        ORDER_ITEM
    }

    class Transformer {
        +float DEFAULT_GST_RATE
        -SparkSession spark
        +flattenOrder(df, schema) DataFrame
        +flattenProduct(df, schema) DataFrame
        +getOrderDataframe(df) DataFrame
        +getProductDataframe(df) DataFrame
        +joinDataframe(order_df, prd_df) DataFrame
        +getReceiptDataframe(df, gst_rate) DataFrame
    }

    DataSource <|-- DummyDataSource

    Pipeline --> PipelineConfig
    Pipeline --> DataSource
    Pipeline --> OutputWriter
    Pipeline --> SchemaRepository
    Pipeline --> Transformer
    SchemaRepository ..> SchemaEnum
```

## Run locally

```bash
cd code
zip -ru9 core.zip core/
cd ..
spark-submit --master local --py-files code/core.zip code/jobs/driver.py
```

## Tests

```bash
cd code
python -m pytest
```
