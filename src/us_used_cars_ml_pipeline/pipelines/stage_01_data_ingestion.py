from pyspark.sql import SparkSession
from us_used_cars_ml_pipeline.config.configuration import ConfigurationManager
from us_used_cars_ml_pipeline.components.data_ingestion import DataIngestion
from us_used_cars_ml_pipeline import logger
from us_used_cars_ml_pipeline.utils.common import get_spark_session


class DataIngestionTrainingPipeline:
    """
    This pipeline is responsible for orchestrating the data ingestion process 
    for training. It initializes necessary configurations, reads data from HDFS, 
    and ensures a smooth flow of the data ingestion stage.
    """
    
    STAGE_NAME = "Data Ingestion State"

    def __init__(self):
        self.config_manager = ConfigurationManager()

    def initialize_spark_session(self) -> SparkSession:
        """Initialize and return a Spark session."""
        return get_spark_session()

    def run_data_ingestion(self):
        """
        Main method to run the data ingestion process.
        """
        try:
            logger.info("Fetching data ingestion configuration...")
            data_ingestion_config = self.config_manager.get_data_ingestion_config()
            
            logger.info("Initializing data ingestion process...")
            data_ingestion = DataIngestion(config=data_ingestion_config)
            
            logger.info("Reading data file...")
            spark = self.initialize_spark_session()
            data_ingestion.read_data_from_hdfs(spark)
        except Exception as e:
            logger.exception("An error occurred during the data ingestion process.")
            raise e

    def run_pipeline(self):
        """
        Run the data ingestion training pipeline.
        """
        try:
            logger.info(f">>>>>> Stage: {DataIngestionTrainingPipeline.STAGE_NAME} started <<<<<<")
            self.run_data_ingestion()
            logger.info(f">>>>>> Stage {DataIngestionTrainingPipeline.STAGE_NAME} completed <<<<<< \n\nx==========x")
        except Exception as e:
            # No need to log the exception here since it's already logged in the run_data_ingestion method.
            raise e

if __name__ == '__main__':
    pipeline = DataIngestionTrainingPipeline()
    pipeline.run_pipeline()
