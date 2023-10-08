from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from us_used_cars_ml_pipeline.entity.config_entity import DataIngestionConfig
from us_used_cars_ml_pipeline import logger

class DataIngestion:
    """
    The DataIngestion component is responsible for managing the reading and ingestion 
    of data from HDFS. This class provides methods to read data from HDFS into Spark 
    DataFrame format. 

    Attributes:
        config (DataIngestionConfig): Configuration parameters for data ingestion.

    Note:
        This class will be expanded in the future to support additional functionalities 
        related to data ingestion.
    """
    
    def __init__(self, config: DataIngestionConfig):
        """
        Initializes the DataIngestion component with the given configuration.

        Args:
            config (DataIngestionConfig): Configuration parameters for data ingestion.
        """
        self.config = config

    def read_data_from_hdfs(self, spark: SparkSession) -> DataFrame:
        """
        Reads data from HDFS and returns it as a DataFrame.

        Args:
            spark (SparkSession): Active SparkSession for data processing.

        Returns:
            DataFrame: Spark DataFrame containing the read data.

        Raises:
            Exception: If there's an error during the data reading process.
        """
        try:
            df = spark.read.csv(self.config.hdfs_data_file, header=True, inferSchema=True)
            # df.show()  # Display the first few rows of the DataFrame
            return df
        except Exception as e:
            logger.error(f"Failed to read data from HDFS. Error: {e}")
            raise e
