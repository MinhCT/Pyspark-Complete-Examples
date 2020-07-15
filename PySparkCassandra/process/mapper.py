import json
import math
import datetime

from json import JSONDecodeError

from pyspark.sql import SparkSession
from pyspark.sql.types import *

import pyspark.sql.functions as F

from util import log_manager
from util import io_utils

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
STORE_ID = 2
EVENT_CLICK = 2


class SparkProcess:

    def __init__(self, app_name, master):
        cassandra_config_file = "settings/cassandra-config.json"
        self.log = log_manager.get_logger(self.__class__.__name__)
        try:
            config = io_utils.read_json(cassandra_config_file)
            self.spark = SparkSession.builder.appName(app_name).master(master) \
                .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.11:2.5.0') \
                .config('spark.cassandra.connection.host', config.host) \
                .config('spark.cassandra.connection.port', config.port) \
                .config('spark.cassandra.auth.username', config.username) \
                .config('spark.cassandra.auth.password', config.password) \
                .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
                .getOrCreate()
        except JSONDecodeError:
            self.log.error("Failed to load Cassandra config json file")
        except FileNotFoundError as f_error:
            self.log.error(f"Can not locate Redis config file '{f_error.filename}'. Failed to initialize Spark Session")
        except TypeError:
            self.log.exception("Invalid json config parsing from file")

    def process(self):
        """
        - Run a Spark job to extract merchandises that have most "Add To Cart" button clicked
        on store having store_id = 1

        :return: none
        """
        if not hasattr(self, "spark") or self.spark is None:
            self.log.info("PySpark has failed to initialize. No mapper will be processed")
            return

        # Define the date range to read from Cassandra
        from_date = datetime.datetime(2020, 1, 1, 0, 0)
        to_date = datetime.datetime.now()

        # - Since the "user_logs" store extremely large data (around tens of millions rows per day),
        # it is hard to store all data on a local computer's RAM
        # - Instead, we will divide the date range in multiple "days", and perform needed
        # aggregations per day, and union into a final results. This will save a lot of RAM
        # and make it possible to read billions of records of event logs using just one normal
        # machine (running Spark in local mode)
        #
        # Below is a process reading 20 billions of rows in the event logs using a 8 GB RAM PC
        diff = to_date - from_date
        days_diff = math.ceil(diff.days + diff.seconds / 86400)
        df_final_results = None

        for i in range(days_diff + 1):
            to = from_date + datetime.timedelta(days=1) - datetime.timedelta(hours=1)
            df_temp = self.create_log_dataframe(from_date, to) \
                .rdd \
                .filter(lambda row: _filter_btn_add_to_cart_clicked(row)) \
                .map(lambda row: _map_to_output(row)) \
                .toDF("date") \
                .groupBy("date") \
                .count()

            if df_final_results is None:
                df_final_results = df_temp
            else:
                df_final_results = df_final_results.union(df_temp)

        df_final_results.show(truncate=False)
        """
        The showed results represent the total Add-To-Cart Button click per day, example:
        
        +--------+--------+
        |  date  |  count |
        +--------+--------+
        |20200613|   13   |
        +--------+--------+
        |20200614|   212  |
        +--------+--------+
        |20200615|   131  |
        +--------+--------+
        |20200616|    1   |
        +--------+--------+
        |20200617|   26   |
        +--------+--------+
        """

    def create_log_dataframe(self, from_date, to_date):
        selected_cols = ["store_id", "year", "month", "day", "hour", "log_time", "url", "detail"]

        df_full_user_logs = self.spark.read.format(CASSANDRA_FORMAT) \
            .options(keyspace="demo_kp", table="user_log") \
            .load() \
            .select(selected_cols) \
            .where(f"event_id = {EVENT_CLICK}")

        df_partition_keys = self.create_partition_dataframe(from_date, to_date)

        # Joining a dataframe with partition keys with Cassandra dataframe will make
        # Spark perform DirectJoin, which is much more efficient comparing to traditional Spark Scanning
        return df_partition_keys.join(df_full_user_logs,
                                      on=(df_partition_keys.store_id == df_full_user_logs.store)
                                         & (df_partition_keys.year == df_full_user_logs.year)
                                         & (df_partition_keys.month == df_full_user_logs.month)
                                         & (df_full_user_logs.day == df_full_user_logs.day)
                                         & (df_partition_keys.hour == df_full_user_logs.hour)
                                      ) \
            .select("log_time", "url", "detail") \
            .filter(F.col("url").rlike("^<some-regex-here>$")) \
            .filter(F.col("detail").isNotNull()) \
            .cache()

    def create_partition_dataframe(self, from_date, to_date):
        partition_keys = self.create_user_logs_partition_keys(from_date, to_date)
        schema = StructType([
            StructField("store_id", IntegerType(), False),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False),
        ])

        return self.spark.createDataFrame(partition_keys, schema)

    def create_user_logs_partition_keys(self, from_date, to_date):
        diff = to_date - from_date
        hours_diff = diff.days * 24 + diff.seconds // 3600

        partition_keys = []
        for i in range(hours_diff + 1):
            partition_keys.append((STORE_ID, from_date.year, from_date.month,
                                   from_date.day, from_date.hour))
            from_date += datetime.timedelta(hours=1)

        return partition_keys


def _filter_btn_add_to_cart_clicked(row):
    detail = row.detail
    if detail:
        dict_detail = json.loads(detail)
        if dict_detail["click_target"]:
            dict_click_target = dict_detail["click_target"]
            if dict_click_target["btn-id"] is "add-to-cart":
                return True

    return False


def _map_to_output(row):
    log_time = row.log_time
    return [convert_to_date_int(log_time)]  # spark dataframe only accepts list, tuples, Row object...


def convert_to_date_int(time_milli):
    d = datetime.datetime.fromtimestamp(time_milli / 1000)
    return int("{}{:02d}{:02d}".format(d.year, d.month, d.day))
