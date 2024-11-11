import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, LongType, ArrayType, MapType, TimestampType,IntegerType
from user_agents import parse
from util.config import Config
from util.logger import Log4j

from dim_table import create_dim_date,create_dim_product,create_dim_territory

# create sparkSession
conf = Config()
spark_conf = conf.spark_conf
kafka_conf = conf.kafka_conf
kafka_conf.update({
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1000",
        "startingOffsets": "earliest",
        "auto.offset.reset": "earliest"
    })

spark = SparkSession.builder \
    .config(conf=spark_conf) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
log = Log4j(spark)

log.info(f"spark_conf: {spark_conf.getAll()}")
log.info(f"kafka_conf: {kafka_conf.items()}")

# create dim date,product,territory
dim_date = create_dim_date(spark)
dim_product = create_dim_product(spark)
dim_territory = create_dim_territory(spark)
# dim_territory.show()



def normalize(df):
    # create structure to transform json to dataframe
    schema = StructType([
    StructField("_id", StringType(), True),
    StructField("time_stamp", LongType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("user_id_db", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("local_time", TimestampType(), True),
    StructField("show_recommendation", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("recommendation", StringType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("option", ArrayType(StructType([
        StructField("option_label", StringType(), True),
        StructField("option_id", StringType(), True),
        StructField("value_label", StringType(), True),
        StructField("value_id", StringType(), True)
    ])), True)
    ])

    df_converted = df.select(from_json(col("value").cast("string"), schema).alias("data"))
    df_final = df_converted.select(
                               "data.time_stamp", 
                               "data.ip", 
                               "data.user_agent",  
                               "data.store_id", 
                               "data.local_time",  
                               "data.current_url", 
                               "data.referrer_url",   
                               "data.product_id", 
                               )
    return df_final


def process_batch(batch_df):
    # generate territory_id
    tmp_df =  batch_df
    current_domain =  split(col('current_url'),'/')[2]
    domain_size = size(split(current_domain,r"\."))
    country_code = (split(current_domain,r"\.").getItem(domain_size-1))
    dim_territory_id = abs(hash(country_code))
    tmp_df = tmp_df\
    .withColumn("tmp_territory_id",dim_territory_id)
    behaviour_df = tmp_df.join(dim_territory,tmp_df["tmp_territory_id"]==dim_territory["territory_id"],'left')
    # territory_id
    gen_territory_id = when(col('territory_id').isNull(),-1).otherwise(col('territory_id'))

    # geneate date_id
    gen_date_id = date_format(col("local_time"),'HHddMMyyyy').cast('int')
    #generate browser_id
    parse_browser_udf = udf(lambda ua:parse(ua).browser.family, returnType=StringType())
    gen_browser_id = abs(hash(col('browser')))

    # generate os_id
    parse_os_udf = udf(lambda ua:parse(ua).os.family, returnType=StringType())
    gen_browser_id = abs(hash(col('os')))


    behaviour_df\
    .withColumn('territory_id',gen_territory_id)\
    .withColumn('date_id',gen_date_id)\
    .withColumn('browser',parse_browser_udf)\
    .withColumn('browser_id',gen_browser_id)\
    .withColumn('os',parse_os_udf)\
    .withColumn('')
    .select('territory_id','date_id')\
    .show(truncate=False)


if __name__ == '__main__':

    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()


    query = df.transform(lambda df: normalize(df)) \
        .writeStream \
        .outputMode('append') \
        .foreachBatch(lambda batch_df,  batch_id:process_batch(batch_df))\
        .option("truncate", False) \
        .trigger(processingTime="4 seconds") \
        .start() \
        

    query.awaitTermination()
