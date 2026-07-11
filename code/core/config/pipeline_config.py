from dataclasses import dataclass

from core.transform.transformer import Transformer


@dataclass(frozen=True)
class PipelineConfig:
    master: str = "local"
    output_path: str = "total_price"
    partition_column: str = "client_name"
    write_mode: str = "overwrite"
    gst_rate: float = Transformer.DEFAULT_GST_RATE
