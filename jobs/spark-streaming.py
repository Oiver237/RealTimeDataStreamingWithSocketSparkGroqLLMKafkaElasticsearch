from groq import Groq
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, when, udf

from config.config import config


def sentiments_analysis(comment) -> str:
    if (comment):

        client = Groq(
            api_key=config['groq']['api_key'],  # This is the default and can be omitted
            )
        # openai.api_key = config['openai']['api_key']
        completion = client.chat.completions.create(
            model='llama3-8b-8192',
            messages = [
                {
                    "role":"system",
                    "content": """
                    You are a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                    You have to respond with one word from the option specified above, do not add anything else.
                    Here is the comment:

                    {comment}

                """.format(comment = comment)
                }
            ]
        )
        return completion.choices[0].message.content
    return "Empty"

def start_streaming(spark):
    topic = 'customers_review'
    kafka_config = config['kafka']  # Access Kafka config once to avoid repeated lookups

    while True:
        try:
            # Reading from socket
            stream_df = (spark.readStream.format("socket")
                         .option("host", "spark-master")
                         .option("port", 9999)
                         .load()
                         )

            # Define schema and parse incoming data
            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias('data')).select("data.*")

            sentiments_analysis_udf = udf(sentiments_analysis, StringType())

            stream_df = stream_df.withColumn('feedback',
                                    when(col('text').isNotNull(), sentiments_analysis_udf(col('text')))
                                    .otherwise(None)
                            )

            # Prepare data for Kafka output
            kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")

            # Writing to Kafka
            query = (kafka_df.writeStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", kafka_config['bootstrap.servers'])
                     .option("kafka.security.protocol", kafka_config['security.protocol'])
                     .option("kafka.sasl.mechanism", kafka_config['sasl.mechanisms'])
                     .option("kafka.sasl.jaas.config",
                             'org.apache.kafka.common.security.plain.PlainLoginModule required '
                             'username="{username}" password="{password}";'.format(
                                 username=kafka_config['sasl.username'],
                                 password=kafka_config['sasl.password']
                             ))
                     .option("checkpointLocation", "/tmp/checkpoint")
                     .option("topic", topic)
                     .outputMode("append")
                     .start()
                     .awaitTermination()
                     )
        except Exception as e:
            print(f"Exception encountered: {e}. Retrying in 10 seconds.")
            sleep(10)


if __name__ == "__main__":
    spark_conn = SparkSession.builder \
        .appName("SocketStreamConsumer") \
        .getOrCreate()
    start_streaming(spark_conn)
