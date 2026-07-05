from core.config.pipeline_config import PipelineConfig
from core.pipeline.my_pipeline import Pipeline

if __name__ == "__main__":

    config = PipelineConfig()
    pipeline = Pipeline(config, verbose=True)
    pipeline.initialiseSpark(config.master)
    pipeline.runPipeline()


