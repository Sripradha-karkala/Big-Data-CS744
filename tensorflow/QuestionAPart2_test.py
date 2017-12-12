import tensorflow as tf
import os
import pdb
# number of features in the criteo dataset after one-hot encoding
num_features = 33762578
eta = 0.01
iterations = 10
test_set = 10
freq = 5

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

def increment_acc():
	return tf.assign_add(total_acc, 1)

def do_nothing():
    return tf.constant(0, dtype=tf.int64)

g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-48-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.ones([num_features,]), name="model", dtype=tf.float32)
        derived_label = tf.Variable(0, name="derived_label", dtype=tf.float32) 
        total_acc = tf.Variable(0, name='Total_acc', dtype=tf.int64)
        file_test_queue = tf.train.string_input_producer(["/home/ubuntu/tensorflow-data/tfrecords22"], num_epochs=None)
        test_reader = tf.TFRecordReader()
        _, serialized_example = test_reader.read(file_test_queue)
        test_features = tf.parse_single_example(serialized_example, 
					features={
						'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                                'index' : tf.VarLenFeature(dtype=tf.int64),
                                                'value' : tf.VarLenFeature(dtype=tf.float32),
						 })
        test_label = test_features['label']
        test_value = test_features['value']
        test_index = test_features['index']
        test_dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(test_index),
                                           [num_features,],
    #                                           tf.constant([33762578, 1], dtype=tf.float32),
                                           tf.sparse_tensor_to_dense(test_value))

    # creating 5 reader operators to be placed on different operators
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    gradients = []
    for i in range(0, 5):
        with tf.device("/job:worker/task:%d" % i):
            # We first define a filename queue comprising 5 files.
            filename_queue = tf.train.string_input_producer(file_distributions[i], num_epochs=None)


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

            # since we parsed a VarLenFeatures, they are returned as SparseTensors.
            # To run operations on then, we first convert them to dense Tensors as below.
            dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                           [num_features,],
#                                           tf.constant([33762578, 1], dtype=tf.float32),
                                           tf.sparse_tensor_to_dense(value))

            mat_mul = tf.reduce_sum(tf.mul(w, dense_feature))
            sigmoid = tf.sigmoid(tf.mul(tf.cast(label, tf.float32), mat_mul))
            local_gradient = tf.mul(tf.cast(label, tf.float32), tf.mul((sigmoid - 1), dense_feature))
            gradients.append(tf.mul(local_gradient, eta))


    # we create an operator to aggregate the local gradients
    with tf.device("/job:worker/task:0"):
        aggregator = tf.add_n(gradients)
        assign_op = tf.assign_sub(w, aggregator)

	derived_label = tf.sign(tf.reduce_sum(tf.mul(w, test_dense_feature)))
        equal_test = tf.equal(tf.reshape(test_label, []), tf.cast(derived_label, tf.int64))
        accuracy = tf.cond(equal_test, increment_acc, do_nothing, name='Accuracy')
	reset_acc_var = total_acc.assign(0)
   
    with tf.Session("grpc://vm-17-1:2222") as sess:
        coord = tf.train.Coordinator()
        sess.run(tf.initialize_all_variables())
        file_threads = tf.train.start_queue_runners(sess=sess, coord = coord)
        while iterations >= 0:
            print 'iteration : ', iterations 
            sess.run(assign_op)
            print w.eval()
	    if iterations % freq == 0:
		sess.run(reset_acc_var)
                for j in range(0, test_set):
		    sess.run(accuracy)
		print "Avg accuracy  is ", float(total_acc.eval())/test_set
            iterations -= 1
        coord.request_stop()
        coord.join(file_threads, stop_grace_period_secs=5)
        sess.close()
