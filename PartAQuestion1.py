from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

if __name__ == '__main__':

    spark = SparkSession \
            .builder \
            .appName('PartAQuestion1') \
            .getOrCreate()

    input_schema = StructType() \
                    .add('userA', StringType()) \
                    .add('userB', StringType()) \
                    .add('timestamp', StringType()) \
                    .add('interaction', StringType())

    tweets = spark \
             .readStream \
             .csv('/home/ubuntu/assign1/partA/streaming_dir', schema=input_schema)

    # Split the tweets to get values
    tweets.show()
