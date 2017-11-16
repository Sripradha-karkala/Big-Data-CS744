# Stopping hdfs and spark 
echo 'Stopping hdfs and spark, if executing'

source ~/run.sh
cd /home/ubuntu
stop_spark
stop_hdfs

echo ' Starting hdfs and spark ... '

start_hdfs
start_spark

echo ' Clearing the output directory /home/ubuntu/assign2/partA/question2_results for previous entries'

hadoop dfs -rm /home/ubuntu/assign2/partA/question2_results/*

echo 'Submitting job to spark -Question 2 Part A'
echo 'Input directory is set to /home/ubuntu/assign2/partA/streaming_dir in hdfs'

~/software/spark-2.1.0-bin-hadoop2.6/bin/spark-submit /home/ubuntu/assign2/spark-streaming/PartAQuestion2.py /home/ubuntu/assign2/partA/streaming_dir
