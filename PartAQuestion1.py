from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import window
import streaming_files
from threading import Thread
import sys

STAGING_DIR='/home/ubuntu/assign2/partA/split-dataset'
WINDOW_DURATION='60 minutes'
UPDATE_DURATION='30 minutes'

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: python PartAQuestion1.py <streaming_dir"
        exit()
    streaming_dir = sys.argv[1]

    streaming_files.initial_setup(STAGING_DIR, streaming_dir)    
    # Start the streaming process
    streaming_thread = Thread(target=streaming_files.start_streaming, args=(STAGING_DIR, streaming_dir))
    streaming_thread.start()

    spark = SparkSession \
            .builder \
            .appName('PartAQuestion1') \
            .getOrCreate()

    input_schema = StructType() \
                    .add('userA', StringType()) \
                    .add('userB', StringType()) \
                    .add('timestamp', TimestampType()) \
                    .add('interaction', StringType())

    tweets = spark \
             .readStream \
             .csv('hdfs:/home/ubuntu/assign2/partA/streaming_dir', schema=input_schema)

    # Split the tweets to get values
    interactions = tweets.groupBy(
			window(tweets.timestamp, WINDOW_DURATION, UPDATE_DURATION),
			tweets.interaction
			).count()


    query = interactions \
            .writeStream \
            .outputMode('complete') \
            .format('console') \
	    .option('truncate', 'false') \
	    .option('numRows', 100000000) \
            .start()

    query.awaitTermination()
    
    #  Run this as a separate thread
    # streaming_thread = Thread(target=streaming_files.start_streaming, args=(STAGING_DIR, streaming_dir))
    # streaming_thread.start()
