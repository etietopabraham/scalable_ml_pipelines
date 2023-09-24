from dataclasses import dataclass

@dataclass(frozen=True)
class DataIngestionConfig:
    """
    Configuration for the data ingestion process.
    
    Attributes:
    - root_dir: The directory where data ingestion artifacts should be stored.
    - source_URL: The source URL from which the dataset is to be downloaded.
    - hdfs_data_file: The path in HDFS where the ingested data file should be stored.
    """
    
    root_dir: str  # Directory for data ingestion related artifacts
    source_URL: str  # URL for the source dataset
    hdfs_data_file: str  # Path in HDFS for the ingested data
