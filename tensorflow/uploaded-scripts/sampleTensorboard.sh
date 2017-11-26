#!/bin/bash
export TF_LOG_DIR="/home/ubuntu/tf/logs"

# run a simple program that generates logs for tensorboard
python exampleMatmulSingle.py

# start the tensorboard web server. If you have started the webserver on the VM
# a public ip, then you can view Tensorboard on the browser on your workstation
# (not the CloudLab VMs). Navigate to http://<publicip>:6006 on your browser and
# look under "GRAPHS" tab.

# Under the "GRAPHS" tab, use the options on the left to navigate to the "Run" you are interested in.
tensorboard --logdir=$TF_LOG_DIR
