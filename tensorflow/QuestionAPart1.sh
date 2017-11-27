#!/bin/bash
export TF_LOG_DIR="/home/ubuntu/tf/logs"
# tfdefs.sh has helper function to start process on all VMs
# it contains definition for start_cluster and terminate_cluster
source uploaded-scripts/tfdefs.sh

# startserver.py has the specifications for the cluster.
start_cluster uploaded-scripts/startserver.py

echo "Executing the distributed tensorflow job from QuestionAPart1.py"
# testdistributed.py is a client that can run jobs on the cluster.
# please read testdistributed.py to understand the steps defining a Graph and
# launch a session to run the Graph
python QuestionAPart1.py

# Under the "GRAPHS" tab, use the options on the left to navigate to the "Run" you are interested in.
tensorboard --logdir=$TF_LOG_DIR
# defined in tfdefs.sh to terminate the cluster
terminate_cluster
