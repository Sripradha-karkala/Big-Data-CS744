import tensorflow as tf
import os

# number of features in the criteo dataset after one-hot encoding
num_features = 33762578
eta = 0.01

file_distributions = [[
    "./data/criteo-tfr-tiny/tfrecords00",
    "./data/criteo-tfr-tiny/tfrecords01",
    "./data/criteo-tfr-tiny/tfrecords02",
    "./data/criteo-tfr-tiny/tfrecords03",
    "./data/criteo-tfr-tiny/tfrecords04",
],
[
    "./data/criteo-tfr-tiny/tfrecords05",
    "./data/criteo-tfr-tiny/tfrecords06",
    "./data/criteo-tfr-tiny/tfrecords07",
    "./data/criteo-tfr-tiny/tfrecords08",
    "./data/criteo-tfr-tiny/tfrecords09",
],
[
    "./data/criteo-tfr-tiny/tfrecords10",
    "./data/criteo-tfr-tiny/tfrecords11",
    "./data/criteo-tfr-tiny/tfrecords12",
    "./data/criteo-tfr-tiny/tfrecords13",
    "./data/criteo-tfr-tiny/tfrecords14",
],
[
    "./data/criteo-tfr-tiny/tfrecords15",
    "./data/criteo-tfr-tiny/tfrecords16",
    "./data/criteo-tfr-tiny/tfrecords17",
    "./data/criteo-tfr-tiny/tfrecords18",
    "./data/criteo-tfr-tiny/tfrecords19",
],
[
    "./data/criteo-tfr-tiny/tfrecords20",
    "./data/criteo-tfr-tiny/tfrecords21",
]]

g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-48-1
    #with tf.device("/job:worker/task:0"):
    #    w = tf.Variable(tf.ones([num_features, 1]), name="model")


    # creating 5 reader operators to be placed on different operators
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    for i in range(0, 5):
        with tf.device("/job:worker/task:%d" % i):
            # We first define a filename queue comprising 5 files.
            filename_queue = tf.train.string_input_producer(file_distributions[i], num_epochs=None)

     #       gradients = []

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
            print label
            print index
            print value

            # since we parsed a VarLenFeatures, they are returned as SparseTensors.
            # To run operations on then, we first convert them to dense Tensors as below.
            dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                           [num_features,],
            #                               tf.constant([33762578, 1], dtype=tf.int64),
                                           tf.sparse_tensor_to_dense(value))


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
            # sigmoid = tf.sigmoid(tf.mul(label, tf.matmul(reader, tf.transpose(w))))
            # local_gradient = tf.mul(label, tf.mul((sigmoid - 1), reader))
            # gradients.append(tf.mul(local_gradient, eta))


    # we create an operator to aggregate the local gradients
    #with tf.device("/job:worker/task:0"):
    #    aggregator = tf.add_n(gradients)
        #
    #    assign_op = w.assign_add(aggregator)


    with tf.Session("grpc://vm-17-1:2222") as sess:
        sess.run(tf.initialize_all_variables())
        tf.train.start_queue_runners(sess=sess)
        for i in range(0, 10):
            output = sess.run(dense_feature)
            #print w.eval()
            print output.shape
            print sum(output)

        #sess.close()
