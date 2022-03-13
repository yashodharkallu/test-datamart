# write a code to read daat from mysql and write it to aws s3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.utils.utilities as ut

if __name__ == '__main__':

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    s3_conf = app_conf['s3_conf']
    datalake_path = 's3a://' + s3_conf['s3_bucket'] + s3_conf['datalake_path']
    source_list = app_conf['source_list']

    for src in source_list:
        if src == 'SB':
            txnDf = ut.read_from_mysql(spark,
                                       app_secret['mysql_conf']['hostname'],
                                       app_secret['mysql_conf']['port'],
                                       app_secret['mysql_conf']['database'],
                                       app_conf['SB']["mysql_conf"]["dbtable"],
                                       app_secret["mysql_conf"]["username"],
                                       app_secret["mysql_conf"]["password"],
                                       app_conf['SB']["mysql_conf"]["partition_column"])
            txnDf = txnDf.withColumn('ins_dt', current_date())

            txnDf.show()
            txnDf.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet(datalake_path + '/' + src)

        elif src == 'OL':
            ol_txn_df = ut.read_from_sftp(spark,
                                          app_secret["sftp_conf"]["hostname"],
                                          app_secret["sftp_conf"]["port"],
                                          app_secret["sftp_conf"]["username"],
                                          os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]),
                                          app_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
            ol_txn_df = ol_txn_df.withColumn('ins_dt', current_date())

            ol_txn_df.show(5, False)
            ol_txn_df.write.parquet(datalake_path + '/' + src)

        elif src == 'ADDR':
            addr_df = ut.read_from_mongodb(spark,
                                                   app_conf["mongodb_config"]["database"],
                                                   app_conf["mongodb_config"]["collection"])
            addr_df = addr_df.withColumn('ins_dt', current_date())

            addr_df.show()

            addr_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet(datalake_path + '/' + src)

        elif src == 'CP':
            cp_df = spark.read.csv('s3a://' + s3_conf['bucket_name'] + '/KC_Extract_1_20171009.csv')
            cp_df = addr_df.withColumn('ins_dt', current_date())

            cp_df.show()

            cp_df.write \
                .mode('append') \
                .partitionBy('ins_dt') \
                .parquet(datalake_path + '/' + src)


# spark-submit --packages "mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4" com/clientname/source_data_loading.py
