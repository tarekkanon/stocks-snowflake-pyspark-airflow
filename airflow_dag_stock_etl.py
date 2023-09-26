import os
import datetime

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, DateType, IntegerType
import snowflake.connector

# Snowflake connection parameters
PASSWORD = "############"
WAREHOUSE = "COMPUTE_WH"
ACCOUNT = "######-######"
USER = "kanon"
DATABASE = "STOCK_MARKET"

# necessary packages to be used in initialization of Spark sessions 
SNOWFLAKE_JDBC = "net.snowflake:snowflake-jdbc:3.13.30"
SPARK_SNOWFLAKE = "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4"
SNOWFLAKE_NAMESPACE = "net.snowflake.spark.snowflake"

# Spark app names
SPARK_APP_NAME = "stock_extract_app"
SPARK_SNOWFLAKE_APP_NAME = "stock_snowflake_app"

# Snowflake staging table
SNOWFLAKE_STOCK_PRICES_TABLE = "STOCK_MARKET.STG_LOAD.STOCKS_PRICES"


# get dag path
DAG_PATH = os.path.dirname(__file__)

# path of CSV files on server
STOCK_CSV_DATA_PATH = f"{DAG_PATH}/data/prices.csv"

# path of parquet files which will be stored for upload
STOCK_CSV_SAVE_FILE_PATH = f"{DAG_PATH}/data/parq/stock_prices.parquet"


@task()
def extract_transform_data():
    """Extract stock price data from the CSV files and transform columns for loading
    """    

    print("Loading... ")

    # initialize spark session
    spark = SparkSession.builder.master("local").appName(SPARK_APP_NAME).getOrCreate()  # type: ignore

    print("Spark Session initiated...")

    # print("Apache Spark Version :" + spark.version)
    # print("Apache Spark Version :" + spark.sparkContext.version)

    # read csv data into Spark data frame
    df = spark.read.format("csv").option("header", "true").load(STOCK_CSV_DATA_PATH)
    print("File Loaded...")

    # drop NA records and fields
    df = df.na.drop()

    # print count of data to check if it is loaded into the data frame
    print("Total records count: " + str(df.count()))

    # show sample records in the data frame
    print(df.show(5))

    # describe the dataset of the schema
    print("Dataset Schema...")
    df.printSchema()

    # now change data type of the fields
    df = df.withColumn("open", df.open.cast(FloatType()))
    df = df.withColumn("close", df.close.cast(FloatType()))
    df = df.withColumn("low", df.low.cast(FloatType()))
    df = df.withColumn("high", df.high.cast(FloatType()))
    df = df.withColumn("volume", df.volume.cast(IntegerType()))
    df = df.withColumn("date", df.date.cast(DateType()))

    # rename columns in order not to conflict with Dimension date table
    df = df.withColumnRenamed("date", "market_date")

    # describe the dataset of the df schema to check updates reflected
    print("Changed Dataset Schema...")
    df.printSchema()

    # show sample records in the data frame after transformation
    print(df.show(5))

    # write the result into a parquet file locally to be loaded using the next task in airflow
    df.write.parquet(STOCK_CSV_SAVE_FILE_PATH) 

    print("Success!")
    spark.stop()


@task()
def load_data_to_snowflake():
    """Loading of data stored as parquet file locally into Snowflake staging tables
        then loading it into the Star schema data model
    """

    print("Loading... ")

    # initialize spark session
    snowflake_spark = (
        SparkSession.builder.appName(SPARK_SNOWFLAKE_APP_NAME)  # type: ignore
        .master("local")
        .config("spark.jars.packages", SNOWFLAKE_JDBC + "," + SPARK_SNOWFLAKE)
    ).getOrCreate()
    print("Spark session initiated....")

    # load the parquet file into df to be inserted to Snowflake
    df_to_use = snowflake_spark.read.parquet(STOCK_CSV_SAVE_FILE_PATH)

    # init a Snowflake connection to handle execution of querys better than Spark do it
    snowflake_conn = snowflake.connector.connect(
        user=USER, password=PASSWORD, account=ACCOUNT
    )

    # truncate stg table on Snowflake for stock prices
    execute_query_snowflake(
        snowflake_conn,
        """
        TRUNCATE TABLE STOCK_MARKET.STG_LOAD.STOCKS_PRICES;
        """,
    )

    # checking stg table count before insert to make sure its empty
    total_rec_before_insert = execute_query_snowflake(
        snowflake_conn,
        "select count(*) total_recs from STOCK_MARKET.STG_LOAD.STOCKS_PRICES",
    )

    print("Total count of records before insert : " + str(total_rec_before_insert))

    # insert transformed data into Snowflake staging table
    insert_data_to_snowflake_table(df_to_use)

    # get count of stg table after insert
    total_rec_after_insert = execute_query_snowflake(
        snowflake_conn,
        "select count(*) total_recs from STOCK_MARKET.STG_LOAD.STOCKS_PRICES",
    )

    print("Total count of records after insert : " + str(total_rec_after_insert))

    # now fill in the delta Dimension table for symbols if there is any new
    total_symbols_new = execute_query_snowflake(
        snowflake_conn,
        """INSERT INTO STOCK_MARKET.DATA_WAREHOUSE.DIM_SYMBOL(SYMBOL_CODE)
            SELECT DISTINCT SYMBOL FROM STOCK_MARKET.STG_LOAD.STOCKS_PRICES AS src
            LEFT JOIN STOCK_MARKET.DATA_WAREHOUSE.DIM_SYMBOL AS tgt
            ON tgt.symbol_code = src.SYMBOL
            WHERE tgt.SYMBOL_CODE IS NULL;""",
    )

    print("Total new symbols added : " + str(total_symbols_new))

    # now update staging table with keys from the dimension table
    stock_stg_table_updated = execute_query_snowflake(
        snowflake_conn,
        """UPDATE STOCK_MARKET.STG_LOAD.STOCKS_PRICES SP
            SET SP.SYMBOL = DS.SYMBOL_KEY
            FROM STOCK_MARKET.DATA_WAREHOUSE.DIM_SYMBOL DS
            WHERE DS.SYMBOL_CODE = SP.SYMBOL;""",
    )

    print("Staging table rows updated : " + str(stock_stg_table_updated))

    # now ingesting data into fact table of the stock prices
    stock_fact_table_updated = execute_query_snowflake(
        snowflake_conn,
        """MERGE INTO STOCK_MARKET.DATA_WAREHOUSE.FACT_STOCK_PRICES FSP
            USING STOCK_MARKET.STG_LOAD.STOCKS_PRICES SP
            ON FSP.MARKET_DATE = SP.MARKET_DATE AND FSP.SYMBOL = SP.SYMBOL
            WHEN NOT MATCHED THEN
            INSERT (MARKET_DATE, SYMBOL, OPEN, CLOSE, LOW, HIGH, VOLUME)
            VALUES
            (SP.MARKET_DATE, SP.SYMBOL, SP.OPEN, SP.CLOSE, SP.LOW, SP.HIGH, SP.VOLUME);""",
    )

    print("Count of records inserted to Stock prices fact table : " + str(stock_fact_table_updated))

    print("Success!")
    snowflake_conn.close()


def execute_query_snowflake(snowflake_conn, query):
    """function to execute queries on Snowflake datawarehouse

    Parameters
    ----------
    snowflake_conn : snowflake connection
        connection to be used for quering
    
    query : str
        the query to be executed

    Returns
    -------
    query execution result (mainly the counts)
    """
    snowflake_cursor = snowflake_conn.cursor()
    try:
        snowflake_cursor.execute(query)
        one_row = snowflake_cursor.fetchone()
    finally:
        snowflake_cursor.close()

    return one_row[0]




def insert_data_to_snowflake_table(df):
    """function for Spark to execute the insert command of the dataframe into Snowflake database

    Parameters
    ----------
    df : Spark dataframe
        the dataframe that will be uploaded to Snowflake

    """
    sfOptions = {
        "sfURL": ACCOUNT + ".snowflakecomputing.com",
        "sfAccount": ACCOUNT,
        "sfUser": USER,
        "sfPassword": PASSWORD,
        "sfWarehouse": WAREHOUSE,
        "sfDatabase": DATABASE,
    }
    df.write.format(SNOWFLAKE_NAMESPACE).options(**sfOptions).option(
        "dbtable", SNOWFLAKE_STOCK_PRICES_TABLE
    ).mode("append").options(header=True).save()


# now we define the DAG that will be utilized by Airflow
with DAG(
    dag_id="extract_transform_stock_data",
    schedule_interval="0 9 * * *",
    start_date=datetime.datetime(2023, 9, 22),
    catchup=False,
    tags=["stock data model"],
) as dag:
    # one task group for the ETL of stock prices
    with TaskGroup(
        "extract_transform_load_stock_data_job",
        tooltip="Extract and Transform and Load stock data from CSV file into Snowflake",
    ) as etl:
        extract_transform = extract_transform_data()
        load_data = load_data_to_snowflake()

        # define order
        extract_transform >> load_data

    etl
