import sys
import subprocess
import time
import os

if __name__ == '__main__':
    ''' The program periodically moves files from staging to the input directory
        which gives the effect of streaming data

        input_dir - Input dir path for storing data for streaming
        staging_dir - Path to the staging dir where data is initially stored
    '''
    if len(sys.argv) != 3:
        print "Usage: python streaming_files <input_dir> <staging_dir>"
        exit();

    input_dir = sys.argv[1]
    staging_dir = sys.argv[2]
    
    
    #Source the required variables
    env_variables = subprocess.check_output('. ~/run.sh; env -0',
                                            shell=True,
                                            executable='/bin/bash')
    # Update the env for this python process
    print env_variables

    os.environ.update(line.partition('=')[::2] for line in env_variables.split('\0'))
    # Get the count of files in staging dir
    count = subprocess.check_output(
            ['hdfs dfs -count ' + staging_dir+ ' | tail -1 | awk -F \' \' \'{print $2}\''],
            shell=True)
    print count
    count = count.strip()

    while int(count) > 0:
        # Get the first file in the staging dir
        filename = subprocess.check_output(
        ['hdfs dfs -ls ' + staging_dir + ' | head -2 | awk -F\' \' \'{print $8}\''],
        shell=True)

        filename = filename.strip()
	print filename
        # Move the file to input_dir
	command = 'hdfs dfs -mv ' + filename + ' ' + input_dir
	print command
        subprocess.call(command, shell=True)
        time.sleep(5)
        # Get the count again
        count = subprocess.check_output(
                ['hdfs dfs -count ' + staging_dir + ' | tail -1 | awk -F \' \' \'{print $2}\''],
                shell=True)
        count = count.strip()
