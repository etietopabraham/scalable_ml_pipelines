from us_used_cars_ml_pipeline.constants import *
from us_used_cars_ml_pipeline.utils.common import read_yaml
from us_used_cars_ml_pipeline import logger
from us_used_cars_ml_pipeline.entity.config_entity import (DataIngestionConfig, 
                                                           CleanDataConfig)

class ConfigurationManager:
    """
    The ConfigurationManager class is responsible for reading and providing 
    configuration settings needed for various stages of the data pipeline.

    Attributes:
    - config (dict): Dictionary holding configuration settings from the config file.
    - params (dict): Dictionary holding parameter values from the params file.
    - schema (dict): Dictionary holding schema information from the schema file.
    """
    
    def __init__(self, 
                 config_filepath=CONFIG_FILE_PATH, 
                 params_filepath=PARAMS_FILE_PATH, 
                 schema_filepath=SCHEMA_FILE_PATH):
        """
        Initializes the ConfigurationManager with configurations, parameters, and schema.

        Parameters:
        - config_filepath (str): Filepath to the configuration file.
        - params_filepath (str): Filepath to the parameters file.
        - schema_filepath (str): Filepath to the schema file.
        """
        self.config = self._read_config_file(config_filepath, "config")
        self.params = self._read_config_file(params_filepath, "params")
        self.schema = self._read_config_file(schema_filepath, "schema")

    def _read_config_file(self, filepath: str, config_name: str) -> dict:
        """
        Reads and returns the content of a configuration file.

        Parameters:
        - filepath (str): The file path to the configuration file.
        - config_name (str): Name of the configuration (used for logging purposes).

        Returns:
        - dict: Dictionary containing the configuration settings.

        Raises:
        - Exception: An error occurred reading the configuration file.
        """
        try:
            return read_yaml(filepath)
        except Exception as e:
            logger.error(f"Error reading {config_name} file: {filepath}. Error: {e}")
            raise

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        """
        Extracts and returns data ingestion configuration settings as a DataIngestionConfig object.

        Returns:
        - DataIngestionConfig: Object containing data ingestion configuration settings.

        Raises:
        - AttributeError: The 'data_ingestion' attribute does not exist in the config file.
        """
        try:
            config = self.config.data_ingestion
            return DataIngestionConfig(
                root_dir=config.root_dir,
                source_URL=config.source_URL,
                hdfs_data_file=config.hdfs_data_file
            )
        except AttributeError as e:
            logger.error("The 'data_ingestion' attribute does not exist in the config file.")
            raise e

    def get_clean_data_config(self) -> CleanDataConfig:
        """
        Extracts and returns data cleaning configuration settings as a CleanDataConfig object.

        Returns:
        - CleanDataConfig: Object containing data cleaning configuration settings.

        Raises:
        - AttributeError: The 'clean_data' attribute does not exist in the config file.
        """
        try:
            config = self.config.clean_data
            return CleanDataConfig(
                root_dir=config.root_dir,
                hdfs_data_file=config.hdfs_data_file,
                clean_data_URL=config.clean_data_URL
            )
        except AttributeError as e:
            logger.error("The 'clean_data' attribute does not exist in the config file.")
            raise e
