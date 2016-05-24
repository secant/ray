import orchpy
import numpy as np
import tensorflow as tf

batch_size = 500
xdim = 5000
h1dim = 5000
h2dim = 5000
ydim = 10000
num_steps = 10
x_in = tf.placeholder(tf.float32, [batch_size, xdim])
h1_in = tf.placeholder(tf.float32, [batch_size, h1dim])
h2_in = tf.placeholder(tf.float32, [batch_size, h2dim])

W1_p = tf.Variable(tf.truncated_normal([xdim, h1dim]))
W1_h = tf.Variable(tf.truncated_normal([h1dim, h1dim]))
W2_p = tf.Variable(tf.truncated_normal([h1dim, h2dim]))
W2_h = tf.Variable(tf.truncated_normal([h2dim, h2dim]))
W3_p = tf.Variable(tf.truncated_normal([h2dim, ydim]))

h1 = tf.matmul(x_in, W1_p) + tf.matmul(h1_in, W1_h)
h2 = tf.matmul(h1_in, W2_p) + tf.matmul(h2_in, W2_h)
y = tf.matmul(h2_in, W3_p)

# monolithic tensorflow code
h1_mono = tf.Variable(tf.zeros([batch_size, h1dim]))
h2_mono = tf.Variable(tf.zeros([batch_size, h2dim]))
inputs_monolithic = [tf.placeholder(tf.float32, [batch_size, xdim]) for _ in range(num_steps)]
y_monolithic = []
for t in range(num_steps):
  h1_mono = tf.matmul(inputs_monolithic[t], W1_p) + tf.matmul(h1_mono, W1_h)
  h2_mono = tf.matmul(h1_mono, W2_p) + tf.matmul(h2_mono, W2_h)
  y_monolithic.append(tf.matmul(h2_mono, W3_p))
# end monolithic tensorflow code

init = tf.initialize_all_variables()
sess = tf.Session()
sess.run(init)

@orchpy.distributed([np.ndarray, np.ndarray], [np.ndarray])
def first_layer(x_val, h1_val):
  return sess.run(h1, feed_dict={x_in: x_val, h1_in: h1_val})

@orchpy.distributed([np.ndarray, np.ndarray], [np.ndarray])
def second_layer(h1_val, h2_val):
  return sess.run(h2, feed_dict={h1_in: h1_val, h2_in: h2_val})

@orchpy.distributed([np.ndarray], [np.ndarray])
def third_layer(h2_val):
  return sess.run(y, feed_dict={h2_in: h2_val})
