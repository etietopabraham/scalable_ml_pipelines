from us_used_cars_ml_pipeline.constants import *
from us_used_cars_ml_pipeline.utils.common import read_yaml
from us_used_cars_ml_pipeline import logger

from us_used_cars_ml_pipeline.entity.config_entity import DataIngestionConfig

class ConfigurationManager:
    def __init__(self, 
                 config_filepath=CONFIG_FILE_PATH, 
                 params_filepath=PARAMS_FILE_PATH, 
                 schema_filepath=SCHEMA_FILE_PATH):
        
        try:
            self.config = read_yaml(config_filepath)
        except Exception as e:
            logger.error(f"Error reading config file: {config_filepath}. Error: {e}")
            raise
        
        try:
            self.params = read_yaml(params_filepath)
        except Exception as e:
            logger.error(f"Error reading params file: {params_filepath}. Error: {e}")
            raise
        
        try:
            self.schema = read_yaml(schema_filepath)
        except Exception as e:
            logger.error(f"Error reading schema file: {schema_filepath}. Error: {e}")
            raise

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion

        data_ingestion_config = DataIngestionConfig(
            root_dir=config.root_dir,
            source_URL=config.source_URL,
            hdfs_data_file=config.hdfs_data_file
        )
        return data_ingestion_config
