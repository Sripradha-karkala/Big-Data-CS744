import tensorflow as tf
import os
import pdb
import time
import logging
# number of features in the criteo dataset after one-hot encoding

logging.basicConfig(level=logging.INFO, filename="output_async", filemode="a+",
                        format="%(asctime)-15s %(levelname)-8s %(message)s")
tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

num_features = 33762578
eta = 0.01
iterations = 200000
test_set = 10000
freq = 1000

file_distributions = [[
    "/home/ubuntu/criteo-tfr/tfrecords00",
    "/home/ubuntu/criteo-tfr/tfrecords01",
    "/home/ubuntu/criteo-tfr/tfrecords02",
    "/home/ubuntu/criteo-tfr/tfrecords03",
    "/home/ubuntu/criteo-tfr/tfrecords04",
],
[
    "/home/ubuntu/tfrecords05",
    "/home/ubuntu/tfrecords06",
    "/home/ubuntu/tfrecords07",
    "/home/ubuntu/tfrecords08",
    "/home/ubuntu/tfrecords09",
],
[
    "/home/ubuntu/tfrecords10",
    "/home/ubuntu/tfrecords11",
    "/home/ubuntu/tfrecords12",
    "/home/ubuntu/tfrecords13",
    "/home/ubuntu/tfrecords14",
],
[
    "/home/ubuntu/tfrecords15",
    "/home/ubuntu/tfrecords16",
    "/home/ubuntu/tfrecords17",
    "/home/ubuntu/tfrecords18",
    "/home/ubuntu/tfrecords19",
],
[
    "/home/ubuntu/tfrecords20",
    "/home/ubuntu/tfrecords21",
]]

def increment_acc():
	return tf.assign_add(total_acc_async, 1)

def do_nothing():
    return tf.constant(0, dtype=tf.int64)

g = tf.Graph()

with g.as_default():

    # creating a model variable on task 0. This is a process running on node vm-48-1
    with tf.device("/job:worker/task:0"):
        w_async = tf.Variable(tf.ones([num_features,]), name="model_async", dtype=tf.float32)
        derived_label_async = tf.Variable(0, name="derived_label_async", dtype=tf.float32) 
        total_acc_async = tf.Variable(0, name='Total_acc_async', dtype=tf.int64)
	file_test_queue = tf.train.string_input_producer(["/home/ubuntu/criteo-tfr/tfrecords22"], num_epochs=None)        
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

	test_indices = test_index.values
        test_values =  test_value.values
        w_small_test = tf.gather(w_async, test_indices)
    # creating 5 reader operators to be placed on different operators
    # here, they emit predefined tensors. however, they can be defined as reader
    # operators as done in "exampleReadCriteoData.py"
    with tf.device("/job:worker/task:%d" % FLAGS.task_index):
            # We first define a filename queue comprising 5 files.
            filename_queue = tf.train.string_input_producer(file_distributions[FLAGS.task_index], num_epochs=None)


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
	    indices = index.values
	    values =  value.values
	    w_small = tf.gather(w_async, indices)
            # since we parsed a VarLenFeatures, they are returned as SparseTensors.
            # To run operations on then, we first convert them to dense Tensors as below.

            mat_mul = tf.reduce_sum(tf.mul(w_small, values))
            sigmoid = tf.sigmoid(tf.mul(tf.cast(label, tf.float32), mat_mul))
            local_gradient = tf.mul(tf.cast(label, tf.float32), tf.mul((sigmoid - 1), values))
#           grad=tf.mul(local_gradient, eta)
#            indices_list.append(indices)


    # we create an operator to aggregate the local gradients
    with tf.device("/job:worker/task:0"):
        assign_op = tf.scatter_sub(w_async,indices, tf.mul(local_gradient, eta))
	derived_label_async = tf.sign(tf.reduce_sum(tf.mul(w_small_test, test_values)))
        equal_test = tf.equal(tf.reshape(test_label, []), tf.cast(derived_label_async, tf.int64))
        accuracy = tf.cond(equal_test, increment_acc, do_nothing, name='Accuracy')
	reset_acc_var = total_acc_async.assign(0)
   
    with tf.Session("grpc://vm-17-%d:2222" % (FLAGS.task_index+1)) as sess:
        if FLAGS.task_index == 0:
            coord = tf.train.Coordinator()
            sess.run(tf.initialize_all_variables())
            file_threads = tf.train.start_queue_runners(sess=sess, coord = coord)
        while iterations >= 0:
            logging.info('Iteration no: %d' %(iterations))
            start_time = time.time()
            sess.run(assign_op)
            end_time = time.time()
            logging.info('Time taken: %f'  %(end_time - start_time))
	    if iterations % freq == 0:
		sess.run(reset_acc_var)
                for j in range(0, test_set):
		    sess.run(accuracy)
		logging.info('Accuracy: %f'  % (float(total_acc_async.eval())/test_set))
           	print w_async.eval()
            iterations -= 1
        coord.request_stop()
        coord.join(file_threads, stop_grace_period_secs=5)
        sess.close()
