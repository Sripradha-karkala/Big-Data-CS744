"""
A sample client used to launch jobs on a Distributed TensorFlow cluster.
In this program, we want to find the sum of traces of 5 random matrices.
Each matrix is generated on a different process in the cluster and its traces
are added together.
"""

import tensorflow as tf
import os
import time

tf.logging.set_verbosity(tf.logging.DEBUG)

N = 100000 # dimension of the matrix
d = 50 # number of splits along one dimension. Thus, we will have 100 blocks
M = int(N / d)


def get_block_name(i, j):
    return "sub-matrix-"+str(i)+"-"+str(j)


def get_intermediate_trace_name(i, j):
    return "inter-"+str(i)+"-"+str(j)

start_time = time.time()
# create a empty graph
g = tf.Graph()

# make the graph we created as the default graph. All the variables and
# operators we create will be added to this graph
with g.as_default():

    # this sets the random seed for operations on the current graph
    tf.set_random_seed(1)

    matrices = {} # a container to hold the operators we just created
    counter = 0
    for i in range(0, d):
        for j in range(0, d):

            with tf.device("/job:worker/task:%d" % (counter % 5)):
                matrix_name = get_block_name(i, j)
                # create a operator that generates a random_normal tensor of the
                # specified dimensions. By placing this statement here, we ensure
                # that this operator is executed on "/job:worker/task:i". By
                # default, TensorFlow chooses the process to which the client
                # connects to. Feel free to experiment with default or alternate
                # placement strategies.
                matrices[matrix_name] = tf.ones([M, M], name=matrix_name)
                counter = counter + 1

    # container to hold operators that calculate the traces of individual
    # matrices.
    intermediate_traces = {}
    for i in range(0, d):
        for j in range(0, d):

            with tf.device("/job:worker/task:%d" % (counter % 5)):
                A = matrices[get_block_name(i, j)]
                B = matrices[get_block_name(i, j)]
                # tf.trace() will create an operator that takes a single matrix
                # as input and calculates it input.
                intermediate_traces[get_block_name(i, j)] = tf.trace(tf.matmul(A, B))
                counter = counter + 1

    # sum all the traces
    with tf.device("/job:worker/task:0"):
        retval = tf.add_n(intermediate_traces.values())

    config = tf.ConfigProto(log_device_placement=True)
    with tf.Session("grpc://vm-17-2:2222", config=config) as sess:
        #start_time = time.time()
        result = sess.run(retval)
        #end_time = time.time()
        tf.train.SummaryWriter("%s/QuestionAPart1" % (os.environ.get("TF_LOG_DIR")), sess.graph)
        sess.close()
        end_time = time.time()
        print "Trace of the big matrix is = ", result
        print "Time taken to execute in mins is = ", (end_time - start_time)/ 60
        print "SUCCESS"
