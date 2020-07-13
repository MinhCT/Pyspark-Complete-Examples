import time

from json import JSONDecodeError

import requests
from pyspark.sql import SparkSession

from service import log_manager
from service.redis_ops import save_dict_to_redis
from util import io_utils

APP_NAME = "FCM-Recommend-Mapper"
SPARK_MASTER = "local[24]"
MAX_TIME_USER_AVAILABLE = 180 * 86400000  # 180 days
CURRENT_MILLI_TIME = int(round(time.time() * 1000))
REDIS_FCM_BRANDMODEL_IIDS_KEY = "dm_fcm_brandmodel_iids"
URL_UPDATE_MAPPED_DATA = "https://log.indianauto.com/DataMiningAPI/fcm-token-map"


class SparkMapper:

    def __init__(self, list_tuple_usid_token):
        cassandra_config_file = "settings/cassandra-config.json"
        self.log = log_manager.get_logger(self.__class__.__name__)
        try:
            config = io_utils.read_json(cassandra_config_file)
            self.spark = SparkSession.builder.appName(APP_NAME).master(SPARK_MASTER) \
                .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.11:2.5.0') \
                .config('spark.cassandra.connection.host', config.host) \
                .config('spark.cassandra.connection.port', str(config.port)) \
                .config('spark.cassandra.auth.username', config.username) \
                .config('spark.cassandra.auth.password', config.password) \
                .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
                .getOrCreate()
            self.list_tuple_usid_token = list_tuple_usid_token
        except JSONDecodeError:
            self.log.error("Failed to load Cassandra config json file")
        except FileNotFoundError as f_error:
            self.log.error(f"Can not locate Redis config file '{f_error.filename}'. Failed to initialize Spark Session")
        except TypeError:
            self.log.exception("Invalid json config parsing from file")

    def map(self):
        """
        - Run a Spark job to map firebase token with user session ids and theirs
        trained model related to brand/model auto attributes
        - Output of the job is saved to Redis containing the brand/model as key
        and firebase tokens list as value

        :return: none
        """
        if not hasattr(self, "spark") or self.spark is None:
            self.log.info("PySpark has failed to initialize. No mapper will be processed")
            return

        df_usid_token = self.spark.createDataFrame(self.list_tuple_usid_token, ['user_session_id', 'fcm_token'])
        df_recommend_data = self.spark.read.format("org.apache.spark.sql.cassandra") \
            .options(keyspace="recommendation_indianauto", table="results") \
            .load() \
            .select("user_session_id", "attr_ids", "time")


        df_token_brand_model = df_usid_token \
            .join(df_recommend_data, df_usid_token.user_session_id == df_recommend_data.user_session_id)
            # .map(lambda row: (row[0][1], row[1][1], row[1][2])) \
            # .filter(lambda row: row[2] + MAX_TIME_USER_AVAILABLE >= CURRENT_MILLI_TIME)

        df_usid_token.explain()
        df_token_brand_model.explain()

        # print(df_token_brand_model.toDF().printSchema())
        #
        # rdd = df_token_brand_model.flatMap(lambda row: _flat_map_brand_model(row))
        # results = rdd.groupByKey().mapValues(_map_values_no_dups).collect()
        #
        # self.log.info(f"Total mapped elements: {len(results)}")
        # self.log.info(results)
        # self.save_results_to_redis(results)

    def save_results_to_redis(self, results):
        """
        Save the brand-model -> list of user session ids dictionary
        to Redis local for later observation

        :param results: a dictionary with key being a pair of brand id
        and model id and value is a list of user session ids

        :return: No return
        """
        results_as_dict = {k: ",".join(v) for (k, v) in results}
        save_dict_to_redis(REDIS_FCM_BRANDMODEL_IIDS_KEY, results_as_dict)
        self.log.info("Saved mapped data to local Redis successfully!")
        # self.send_update_to_server(results_as_dict)

    def send_update_to_server(self, dict_json, count_per_request=100):
        """
        Send multiple post requests to server to update production Redis Cluster server

        :param dict_json: a dictionary representing as a request body
        :param count_per_request: use this to divide one request to multiple ones
        to avoid high load to production server API

        :return: No return
        """
        temp_dict = {}
        success = 0

        def _process_request():
            response = requests.post(URL_UPDATE_MAPPED_DATA, json=temp_dict)
            json_resp = response.json()
            if response.status_code == 200 and json_resp and "message" in json_resp \
                    and json_resp["message"] == "OK":
                self.log.info("Process Mapper has sent POST request to update mapped data successfully")
                return 1
            else:
                self.log.info(
                    f"Something not right. Response body from API: {response.content}"
                    f" with status code = {response.status_code}")
                return 0

        if len(dict_json) <= count_per_request:
            temp_dict = dict_json
        else:
            for k, v in dict_json.items():
                temp_dict[k] = v
                if len(temp_dict) == count_per_request:
                    success += _process_request()

        if temp_dict:
            success += _process_request()

        self.log.info(f"Updated mapped data with "
                      f"{success}/{dict_json / count_per_request + 1} successful POST requests")


def _flat_map_brand_model(row):
    """
    Flat the given Spark data frame Row to multiple rows
    Each Row has 2 columns: firebase instance ID (fcm_token) and a list of attribute IDs (attr_ids)
    Logic is to flat the row to many rows having structure:
        [brandId_modelId, list of firebase instance IDs] (2 columns)

    :param row: a Spark dataframe Row
    :return: a list of row after flatting
    """
    top_count = 3
    brand_shift = 11
    model_shift = 22
    brand_mask = 0x7ff
    model_mask = 0xffff
    fcm_token = row[0]
    attr_ids = row[1]

    temp_dict = {}
    for attr_id in attr_ids[:top_count]:
        brand_id = (attr_id >> brand_shift) & brand_mask
        model_id = (attr_id >> model_shift) & model_mask
        temp_dict[f"{brand_id}_{model_id}"] = fcm_token

    return [(k, v) for k, v in temp_dict.items()]


def _map_values_no_dups(iterable):
    """
    Remove duplicates from an iterable object

    :param iterable: an iterable object (can be list, set, etc.)
    :return: a list of unique elements
    """
    return list(set(iterable))
