from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit
from os import environ
from lib.spark import start_spark


def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config['steps_per_floor'])
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def extract_data(spark):
    """Load data from Parquet file format.
    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark
        .read
        .csv('dataset/covid19/download.csv', header=True, inferSchema=True))

    return df

def transform_data(df, steps_per_floor_):
    """Transform original dataset.
    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    df_transformed = (
        df
        .select(
            col('dateRep')))

    return df_transformed


def load_data(df):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    (df
      .write
      .csv('reports', mode='overwrite', header=True))
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
