"""
Main execution script for the entire ML workflow, starting from data ingestion 
to data transformation. Each stage in the workflow is represented as a pipeline 
and is executed sequentially.
"""

# Module imports
from us_used_cars_ml_pipeline import logger
from us_used_cars_ml_pipeline.pipelines.stage_01_data_ingestion import DataIngestionTrainingPipeline
from us_used_cars_ml_pipeline.pipelines.stage_02_data_cleaning import DataCleaningPipeline


def main():
    """
    Main execution method for the data processing and training pipelines.
    Executes pipelines in the predefined sequence.
    """

    # List of pipeline stages to be executed in sequence
    execution_sequence = [DataIngestionTrainingPipeline(), DataCleaningPipeline()]

    for pipeline in execution_sequence:
        try:
            logger.info(f">>>>>> Stage: {pipeline.STAGE_NAME} started <<<<<<")
            pipeline.run_pipeline()  # consistent method name across pipelines for execution
            logger.info(f">>>>>> Stage {pipeline.STAGE_NAME} completed <<<<<< \n\nx==========x")
        except Exception as e:
            logger.exception(f"Error encountered during the {pipeline.STAGE_NAME}: {e}")
            logger.error("Program terminated due to an error.")
            exit(1)

if __name__ == "__main__":
    main()
