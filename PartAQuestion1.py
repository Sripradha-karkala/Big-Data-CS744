from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession \
            .builder \
            .appName('PartAQuestion1') \
            .getOrCreate()

    tweets = spark \
             .readStream \
             .format('csv') \
             .path('/home/ubuntu/assign1/partA/streaming_dir') \
             .load()

    # Split the tweets to get values
    tweets.show()
    
