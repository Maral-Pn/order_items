from code.core.pipeline.my_pipeline import Pipeline

if __name__ == "__main__":

    pipline = Pipeline()
    pipline.initialiseSpark("local")
    pipline.runPipeline()


