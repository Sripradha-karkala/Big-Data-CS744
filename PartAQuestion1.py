from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType

if __name__ == '__main__':

    '''sc = SparkContext(appName='PartAQuestion1')
    ssc = StreamingContext(sc, 1)

    tweets = ssc.textFileStream('hdfs:/home/ubuntu/assign2/partA/streaming_dir')
    # counts = tweets.flatMap(lambda line: line.split(","))\
                  .map(lambda x: (x, 1))\
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()'''

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
    retweets = tweets.groupBy('interaction').count()

    query = retweets \
            .writeStream \
            .outputMode('complete') \
            .format('console') \
            .start()

    query.awaitTermination()
