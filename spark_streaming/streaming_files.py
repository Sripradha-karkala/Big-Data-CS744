import sys
import subprocess
import time
import os

LOCAL_PATH = '/home/ubuntu/assign2/spark-streaming/split-dataset'
def initial_setup(staging_dir, streaming_dir):
   # Initial setup
    #Source the required variables
    env_variables = subprocess.check_output('. ~/run.sh; env -0',
                                            shell=True,
                                            executable='/bin/bash')
    os.environ.clear()
    os.environ.update(line.partition('=')[::2] for line in env_variables.split('\0'))

    # Copy files from local to staging dir
    
    subprocess.call('hadoop fs -rmr ' + staging_dir + '/*',
                    shell=True)

    print 'Copying files from local to staging environment'

    subprocess.call('hadoop fs -copyFromLocal ' + LOCAL_PATH + '/* ' +staging_dir,
                    shell=True)
    # Clear any files already present in the streaming dir
    subprocess.call('hadoop fs -rmr ' + streaming_dir + '/*',
                    shell=True)
 
def start_streaming(staging_dir, streaming_dir):
    # Initial setup
    #Source the required variables
    env_variables = subprocess.check_output('. ~/run.sh; env -0',
                                            shell=True,
                                            executable='/bin/bash')

    # os.environ.clear()
    os.environ.update(line.partition('=')[::2] for line in env_variables.split('\0'))


    # Get the count of files in staging dir
    count = subprocess.check_output(
            ['hdfs dfs -count ' + staging_dir+ ' | tail -1 | awk -F \' \' \'{print $2}\''],
            shell=True)
    count = count.strip()

    while int(count) > 0:
        # Get the first file in the staging dir
        filename = subprocess.check_output(
        ['hdfs dfs -ls ' + staging_dir + ' | head -2 | awk -F\' \' \'{print $8}\''],
        shell=True)

        filename = filename.strip()
        # Move the file to input_dir
        command = 'hdfs dfs -mv ' + filename + ' ' + streaming_dir
        subprocess.call(command, shell=True)
        time.sleep(5)
        # Get the count again
        count = subprocess.check_output(
                ['hdfs dfs -count ' + staging_dir + ' | tail -1 | awk -F \' \' \'{print $2}\''],
                shell=True)
        count = count.strip()
