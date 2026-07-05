from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    master: str = "local"
    output_path: str = "total_price"
    partition_column: str = "client_name"
    write_mode: str = "overwrite"
    gst_rate: float = 1.03
