"""
common.py

Purpose:
    Contains common functionalities used across the project.
"""

from pathlib import Path
from typing import Any, List
import os
import yaml
import json
import joblib

from box import ConfigBox
from box.exceptions import BoxValueError
from ensure import ensure_annotations
from typing import Union

from us_used_cars_ml_pipeline import logger

from pyspark.sql import SparkSession
from hdfs import InsecureClient


@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """
    Reads a yaml file, and returns a ConfigBox object.

    Args:
        path_to_yaml (Path): Path to the yaml file.

    Raises:
        ValueError: If the yaml file is empty.
        e: If any other exception occurs.

    Returns:
        ConfigBox: The yaml content as a ConfigBox object.
    """
    try:
        with open(path_to_yaml) as yaml_file:
            content = yaml.safe_load(yaml_file)
            logger.info(f"yaml file: {path_to_yaml} loaded successfully")
            return ConfigBox(content)
    except BoxValueError:
        logger.info("Value exception: empty yaml file")
        raise ValueError("yaml file is empty")
    except Exception as e:
        logger.info(f"An exception {e} has occurred")
        raise e


@ensure_annotations
def create_directories(path_to_directories: list, verbose=True):
    """
    Create a list of directories.

    Args:
        path_to_directories (List[Path]): List of paths of directories.
        verbose (bool, optional): Log when directory is created. Defaults to True.
    """
    for path in path_to_directories:
        try:
            os.makedirs(path, exist_ok=True)
            if verbose:
                logger.info(f"Created directory at: {path}")
        except PermissionError:
            logger.error(f"Permission denied to create directory at {path}")
            raise
        except OSError as e:
            logger.error(f"Failed to create directory at {path}. Error: {e}")
            raise


@ensure_annotations
def save_json(path: Path, data: dict):
    """
    Save json data

    Args:
        path (Path): path to json file
        data (dict): data to be saved in json file
    """
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"json file saved at: {path}")
    except PermissionError:
        logger.error(f"Permission denied to write to {path}")
        raise
    except OSError as e:
        logger.error(f"Failed to save json to {path}. Error: {e}")
        raise


@ensure_annotations
def load_json(path: Path) -> ConfigBox:
    """
    Load json files data

    Args:
        path (Path): path to json file

    Returns:
        ConfigBox: data as class attributes instead of dict
    """
    try:
        with open(path, "r") as f:
            content = json.load(f)
        logger.info(f"json file loaded successfully from: {path}")
        return ConfigBox(content)
    except FileNotFoundError:
        logger.error(f"File not found at {path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from {path}")
        raise
    except PermissionError:
        logger.error(f"Permission denied to read {path}")
        raise
    except OSError as e:
        logger.error(f"Failed to load json from {path}. Error: {e}")
        raise


@ensure_annotations
def save_bin(data: Any, path: Path):
    """
    Save binary file

    Args:
        data (Any): data to be saved as binary
        path (Path): path to binary file
    """
    try:
        joblib.dump(value=data, filename=path)
        logger.info(f"binary file saved at: {path}")
    except PermissionError:
        logger.error(f"Permission denied to write to {path}")
        raise
    except OSError as e:
        logger.error(f"Failed to save binary to {path}. Error: {e}")
        raise


@ensure_annotations
def load_bin(path: Path) -> Any:
    """
    Load binary data

    Args:
        path (Path): path to binary file

    Returns:
        Any: object stored in the file
    """
    try:
        data = joblib.load(path)
        logger.info(f"binary file loaded from: {path}")
        return data
    except FileNotFoundError:
        logger.error(f"File not found at {path}")
        raise
    except PermissionError:
        logger.error(f"Permission denied to read {path}")
        raise
    except OSError as e:
        logger.error(f"Failed to load binary from {path}. Error: {e}")
        raise


@ensure_annotations
def get_size(path: Path) -> str:
    """
    Get size in KB

    Args:
        path (Path): path to the file

    Returns:
        str: size in KB
    """
    try:
        size_in_kb = round(os.path.getsize(path) / 1024)
        return (f"~ {size_in_kb} KB")
    except FileNotFoundError:
        logger.error(f"File not found at {path}")
        raise
    except PermissionError:
        logger.error(f"Permission denied to access {path}")
        raise
    except OSError as e:
        logger.error(f"Failed to get size for {path}. Error: {e}")
        raise

@ensure_annotations
def get_spark_session(app_name="us_used_cars_app"):
    """
    Singleton pattern to ensure only one SparkSession instance.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


@ensure_annotations
def create_hdfs_directory(directory_path, hdfs_url, hdfs_user):
    """
    Create a directory in HDFS.

    Args:
    - directory_path (str): The path of the directory to be created in HDFS.
    - hdfs_url (str): The URL of the HDFS instance.
    - hdfs_user (str): The HDFS user.

    Returns:
    None
    """
    client = InsecureClient(hdfs_url, user=hdfs_user)
    if not client.status(directory_path, strict=False):
        client.makedirs(directory_path)



@ensure_annotations
def read_data_from_hdfs(spark: SparkSession, hdfs_path: str):
    """
    Reads the data from HDFS and returns it as a DataFrame.
    """
    try:
        df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        df.show()
        return df
    except Exception as e:
        logger.error(f"Failed to read data from HDFS. Error: {e}")
        raise e
