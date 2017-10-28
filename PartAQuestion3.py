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
        print "Usage: python PartAQuestion3.py <streaming_dir> <input_file>"
        exit()
    streaming_dir = sys.argv[1]
    input_file = sys.argv[2]

    streaming_files.initial_setup(STAGING_DIR, streaming_dir)

    # Start the streaming process
    streaming_thread = Thread(target=streaming_files.start_streaming, args=(STAGING_DIR, streaming_dir))
    streaming_thread.start()

    spark = SparkSession \
            .builder \
            .appName('PartAQuestion3') \
            .getOrCreate()


    input_schema = StructType() \
                    .add('userA', StringType()) \
                    .add('userB', StringType()) \
                    .add('timestamp', TimestampType()) \
                    .add('interaction', StringType())

    # Read the static input file
    required_users = spark \
                    .read \
                    .csv(input_file, schema=input_schema)


    tweets = spark \
             .readStream \
             .csv(streaming_dir, schema=input_schema)

    # Get the userids mentioned by other users
    active_users = tweets \
                    .join(required_users, 'userA') \
                    .groupBy('userA')\
                    .count()


    query = userids \
            .writeStream \
            .outputMode('complete') \
            .trigger(processingTime='5 seconds') \
            .format('console') \
        .start()

    query.awaitTermination()
