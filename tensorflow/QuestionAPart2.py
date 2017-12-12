import tensorflow as tf
import os
import pdb
# number of features in the criteo dataset after one-hot encoding
num_features = 33762578
eta = 0.01

file_distributions = [[
    "/home/ubuntu/tensorflow-data/tfrecords00",
    "/home/ubuntu/tensorflow-data/tfrecords01",
    "/home/ubuntu/tensorflow-data/tfrecords02",
    "/home/ubuntu/tensorflow-data/tfrecords03",
    "/home/ubuntu/tensorflow-data/tfrecords04",
],
[
    "/home/ubuntu/tensorflow-data/tfrecords05",
    "/home/ubuntu/tensorflow-data/tfrecords06",
    "/home/ubuntu/tensorflow-data/tfrecords07",
    "/home/ubuntu/tensorflow-data/tfrecords08",
    "/home/ubuntu/tensorflow-data/tfrecords09",
],
[
    "/home/ubuntu/tensorflow-data/tfrecords10",
    "/home/ubuntu/tensorflow-data/tfrecords11",
    "/home/ubuntu/tensorflow-data/tfrecords12",
    "/home/ubuntu/tensorflow-data/tfrecords13",
    "/home/ubuntu/tensorflow-data/tfrecords14",
],
[
    "/home/ubuntu/tensorflow-data/tfrecords15",
    "/home/ubuntu/tensorflow-data/tfrecords16",
    "/home/ubuntu/tensorflow-data/tfrecords17",
    "/home/ubuntu/tensorflow-data/tfrecords18",
    "/home/ubuntu/tensorflow-data/tfrecords19",
],
[
    "/home/ubuntu/tensorflow-data/tfrecords20",
    "/home/ubuntu/tensorflow-data/tfrecords21",
]]

g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-48-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.ones([num_features,]), name="model", dtype=tf.float32)


    # creating 5 reader operators to be placed on different operators
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    gradients = []
    for i in range(0, 5):
        with tf.device("/job:worker/task:%d" % i):
            # We first define a filename queue comprising 5 files.
            filename_queue = tf.train.string_input_producer(file_distributions[i], num_epochs=None)

     	    #gradients = []

            # TFRecordReader creates an operator in the graph that reads data from queue
            reader = tf.TFRecordReader()

            # Include a read operator with the filenae queue to use. The output is a string
            # Tensor called serialized_example
            _, serialized_example = reader.read(filename_queue)


            # The string tensors is essentially a Protobuf serialized string. With the
            # following fields: label, index, value. We provide the protobuf fields we are
            # interested in to parse the data. Note, feature here is a dict of tensors
            features = tf.parse_single_example(serialized_example,
                                               features={
                                                'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                                'index' : tf.VarLenFeature(dtype=tf.int64),
                                                'value' : tf.VarLenFeature(dtype=tf.float32),
                                               }
                                              )

            label = features['label']
            index = features['index']
            value = features['value']

            # These print statements are there for you see the type of the following
            # variables
#            print label
#            print index
#            print value
            #print tf.size(features)
            #print features
            
            # since we parsed a VarLenFeatures, they are returned as SparseTensors.
            # To run operations on then, we first convert them to dense Tensors as below.
            dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                           [num_features,],
#                                           tf.constant([33762578, 1], dtype=tf.float32),
                                           tf.sparse_tensor_to_dense(value))

            #print "dense featire is" 
	    #print dense_feature
            #print "w vector is "
	    #print repr(w)
	    #print "w shape is"
	    #print  w.get_shape()
            # as usual we create a session.
    #sess = tf.Session()
    #sess.run(tf.initialize_all_variables())

            # this is new command and is used to initialize the queue based readers.
            # Effectively, it spins up separate threads to read from the files
    #tf.train.start_queue_runners(sess=sess)

            #reader = tf.ones([10, 1], name="operator_%d" % i)
            # not the gradient compuation here is a random operation. You need
            # to use the right way (as described in assignment 3 desc).
            # we use this specific example to show that gradient computation
            # requires use of the model

	    #pdb.set_trace()	
#            print tf.rank(w)
#            print tf.rank(dense_feature)
#            tf.reshape(dense_feature, [num_features, 1])
            mat_mul = tf.reduce_sum(tf.mul(w, dense_feature))
#	    matmul = tf.tensordot(w,dense_feature,axes=1)
            #mat_mul = tf.matmul(dense_feature, tf.transpose(w))
            sigmoid = tf.sigmoid(tf.mul(tf.cast(label, tf.float32), mat_mul))
            local_gradient = tf.mul(tf.cast(label, tf.float32), tf.mul((sigmoid - 1), dense_feature))
            gradients.append(tf.mul(local_gradient, eta))
            #print 'gradients:', gradients


    # we create an operator to aggregate the local gradients
    with tf.device("/job:worker/task:0"):
        aggregator = tf.add_n(gradients)
        #print aggregator
	#pdb.debugger()
        #
        assign_op = w.assign_sub(aggregator)


    with tf.Session("grpc://vm-17-1:2222") as sess:
        coord = tf.train.Coordinator()
        sess.run(tf.initialize_all_variables())
        file_threads = tf.train.start_queue_runners(sess=sess, coord = coord)
        for i in range(0, 20):
            print 'iteration : ', i 
            sess.run(assign_op)
            print w.eval()
            #print sum(output)
         #   sess.run(assign_op)
            #print w.eval()
            #print output.shape
            #print sum(output)
        coord.request_stop()
        coord.join(file_threads, stop_grace_period_secs=5)
        sess.close()
