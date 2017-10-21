from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
import streaming_files
from threading import thread

STAGING_DIR='/home/ubuntu/assign2/partA/split-dataset'

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: python PartAQuestion1.py <streaming_dir"
        exit()
    streaming_dir = sys.argv[1]

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
             .csv('hdfs:/home/ubuntu/assign2/partA/streaming_dir', schema=input_schema)

    # Split the tweets to get values
    interactions = tweets.groupBy('interaction').count()

    query = interactions \
            .writeStream \
            .outputMode('complete') \
            .format('console') \
            .start()

    query.awaitTermination()

    #  Run this as a separate thread
    streaming_thread = Thread(target=streaming_files.start_streaming, args=(STAGING_DIR, streaming_dir))
    streaming_thread.start()
