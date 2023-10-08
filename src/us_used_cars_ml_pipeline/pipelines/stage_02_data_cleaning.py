from us_used_cars_ml_pipeline.config.configuration import ConfigurationManager
from pyspark.sql import SparkSession
from us_used_cars_ml_pipeline.components.data_cleaning import CleanData
from us_used_cars_ml_pipeline import logger
from us_used_cars_ml_pipeline.utils.common import get_spark_session

class DataCleaningPipeline:
    """
    Pipeline for cleaning the used cars dataset.

    Class Attributes:
    - STAGE_NAME (str): Stage name for logging purposes.

    Attributes:
    - config_manager (ConfigurationManager): Manager for configuration settings.
    """
    
    STAGE_NAME = "Data Cleaning Stage"

    def __init__(self):
        """
        Initializes the DataCleaningPipeline object with the ConfigurationManager.
        """
        self.config_manager = ConfigurationManager()

    def initialize_spark_session(self) -> SparkSession:
        """
        Initializes and returns a Spark session.
        
        Returns:
        - SparkSession: Initialized Spark session.
        """
        return get_spark_session()

    def run_data_cleaning(self):
        """
        Runs the data cleaning process.
        
        This method fetches the data cleaning configuration, initializes the data cleaning process,
        reads the data, and performs data cleaning using the CleanData component.
        """
        try:
            logger.info("Fetching data cleaning configuration...")
            data_cleaning_configuration = self.config_manager.get_clean_data_config()
            
            logger.info("Initializing data cleaning process...")
            data_cleaning = CleanData(config=data_cleaning_configuration)

            logger.info("Read data for cleaning process...")
            spark = self.initialize_spark_session()
            df = data_cleaning.read_data_from_hdfs(spark)

            logger.info("Perform data cleaning")
            cleaned_df = data_cleaning.perform_cleaning(df)
            
            hdfs_path = data_cleaning_configuration.clean_data_URL
            if not hdfs_path:  # or any other validation check you find appropriate
                logger.error("Invalid HDFS path in configuration. Aborting data cleaning process.")
                raise ValueError("Invalid HDFS path in configuration.")
            logger.info(f"Writing cleaned data back to HDFS at {hdfs_path}")
            cleaned_df.write.mode('overwrite').parquet(hdfs_path)
            
        except Exception as e:
            logger.exception("An error occurred during the data cleaning process.")
            raise e
        
    def run_pipeline(self):
        """
        Runs the Data Cleaning Pipeline, logging the start and completion of the stage.
        """
        try:
            logger.info(f">>>>>> Stage: {DataCleaningPipeline.STAGE_NAME} started <<<<<<")
            self.run_data_cleaning()
            logger.info(f">>>>>> Stage {DataCleaningPipeline.STAGE_NAME} completed <<<<<< \n\nx==========x")
        except Exception as e:
            # No need to log the exception here since it's already logged in the run_data_cleaning method.
            raise e

if __name__ == '__main__':
    pipeline = DataCleaningPipeline()
    pipeline.run_pipeline()
