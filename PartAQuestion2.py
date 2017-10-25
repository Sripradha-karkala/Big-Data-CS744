from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import window
import streaming_files
from threading import Thread
import sys

STAGING_DIR='/home/ubuntu/assign2/partA/split-dataset'

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: python PartAQuestion2.py <streaming_dir>"
        exit()
    streaming_dir = sys.argv[1]

    streaming_files.initial_setup(STAGING_DIR, streaming_dir)

    # Start the streaming process
    streaming_thread = Thread(target=streaming_files.start_streaming, args=(STAGING_DIR, streaming_dir))
    streaming_thread.start()

    spark = SparkSession \
            .builder \
            .appName('PartAQuestion2') \
            .getOrCreate()

    input_schema = StructType() \
                    .add('userA', StringType()) \
                    .add('userB', StringType()) \
                    .add('timestamp', TimestampType()) \
                    .add('interaction', StringType())

    tweets = spark \
             .readStream \
             .csv(streaming_dir, schema=input_schema)

    # Get the userids mentioned by other users
    userids = tweets.select('userB')\
                    .where('interaction  == MT')


    query = userids \
            .writeStream \
            .outputMode('append') \
            .trigger(processingTime='10 seconds') \
            .format('parquet') \
	    .option('truncate', 'false') \
	    .option('path', 'hdfs:/home/ubuntu/assign2/partA/question2.csv') \
        .start()

    query.awaitTermination()
