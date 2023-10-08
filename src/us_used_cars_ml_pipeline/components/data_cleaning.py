from pyspark.sql import DataFrame
from us_used_cars_ml_pipeline.entity.config_entity import CleanDataConfig
from us_used_cars_ml_pipeline import logger
from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, BooleanType


class CleanData:
    """
    Class for cleaning the used cars dataset.

    Attributes:
    - config (CleanDataConfig): Configuration for the data cleaning process.
    """

    def __init__(self, config: CleanDataConfig):
        """
        Initializes the CleanData object with the given configuration.

        Parameters:
        - config (CleanDataConfig): Configuration for the data cleaning process.
        """
        self.config = config

    def read_data_from_hdfs(self, spark: SparkSession) -> DataFrame:
        """
        Reads the data from HDFS using the provided SparkSession.

        Parameters:
        - spark (SparkSession): SparkSession object.

        Returns:
        - DataFrame: Spark DataFrame containing the read data.
        """
        try:
            df = spark.read.csv(self.config.hdfs_data_file, header=True, inferSchema=True)
            return df
        except Exception as e:
            logger.error(f"Failed to read data from HDFS. Error: {e}")
            raise e

    def to_float(self, col):
        """
        Utility function to extract float from strings with units.

        Parameters:
        - col (Column): Spark DataFrame column.

        Returns:
        - Column: Transformed column with float values.
        """
        return F.regexp_extract(col, r"(\d+\.?\d*)", 1).cast(FloatType())

    def to_int(self, col):
        """
        Utility function to extract integer from strings.

        Parameters:
        - col (Column): Spark DataFrame column.

        Returns:
        - Column: Transformed column with integer values.
        """
        return F.regexp_extract(col, r"(\d+)", 1).cast(IntegerType())

    def to_bool(self, col):
        """
        Utility function to convert to boolean.

        Parameters:
        - col (Column): Spark DataFrame column.

        Returns:
        - Column: Transformed column with boolean values.
        """
        return col.cast(BooleanType())

    def split_power_torque(self, df, col_name):
        """
        Utility function to split power and torque into value and rpm.

        Parameters:
        - df (DataFrame): Spark DataFrame.
        - col_name (str): Name of the column to split.

        Returns:
        - DataFrame: DataFrame with new columns for value and rpm.
        """
        value = F.regexp_extract(df[col_name], r"(\d+)", 1).cast(IntegerType())
        rpm = F.regexp_replace(F.regexp_extract(df[col_name], r"@ ([\d,]+)", 1), ",", "").cast(IntegerType())
        return df.withColumn(f"{col_name}_value", value).withColumn(f"{col_name}_rpm", rpm)

    def perform_cleaning(self, df: DataFrame) -> DataFrame:
        """
        Performs cleaning operations on the input DataFrame.

        Parameters:
        - df (DataFrame): Input Spark DataFrame.

        Returns:
        - DataFrame: Cleaned DataFrame.
        """
        conversion_dict = {
            'back_legroom': self.to_float,
            'bed_height': self.to_float,
            'bed_length': self.to_float,
            'front_legroom': self.to_float,
            'height': self.to_float,
            'length': self.to_float,
            'wheelbase': self.to_float,
            'width': self.to_float,
            'city_fuel_economy': self.to_float,
            'combine_fuel_economy': self.to_float,
            'daysonmarket': self.to_int,
            'engine_displacement': self.to_float,
            'fuel_tank_volume': self.to_float,
            'highway_fuel_economy': self.to_float,
            'horsepower': self.to_int,
            'latitude': self.to_float,
            'longitude': self.to_float,
            'mileage': self.to_float,
            'owner_count': self.to_int,
            'price': self.to_float,
            'savings_amount': self.to_float,
            'seller_rating': self.to_float,
            'year': self.to_int,
            'fleet': self.to_bool,
            'frame_damaged': self.to_bool,
            'franchise_dealer': self.to_bool,
            'has_accidents': self.to_bool,
            'isCab': self.to_bool,
            'is_certified': self.to_bool,
            'is_cpo': self.to_bool,
            'is_new': self.to_bool,
            'is_oemcpo': self.to_bool,
            'salvage': self.to_bool,
            'theft_title': self.to_bool
        }

        # Apply conversion functions to corresponding columns
        for col, func in conversion_dict.items():
            df = df.withColumn(col, func(df[col]))

        # Convert listed_date to DateType
        df = df.withColumn('listed_date', F.to_date(df['listed_date'], 'yyyy-MM-dd'))

        # Split power and torque into value and rpm, and add new columns
        df = self.split_power_torque(df, 'power')
        df = self.split_power_torque(df, 'torque')

        # Cleaning maximum_seating and converting to Integer
        df = df.withColumn('maximum_seating', F.regexp_replace(F.col('maximum_seating'), '[^\d]+', '').cast(IntegerType()))

        return df  # Return cleaned DataFrame
