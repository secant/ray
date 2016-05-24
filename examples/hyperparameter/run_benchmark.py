"""
./scheduler 52.50.92.141:10001

import orchpy.services as services
test_path = "/home/ubuntu/orch/examples/hyperparameter/benchmark.py"
services.start_node("52.50.92.141:10001", "52.50.92.141", 1, worker_path=test_path)

import orchpy.services as services
test_path = "/home/ubuntu/halo/examples/tensorflow/benchmark.py"
services.start_node("52.50.92.141:10001", "52.49.170.133", 1, worker_path=test_path)

import orchpy.services as services
test_path = "/home/ubuntu/halo/examples/tensorflow/benchmark.py"
services.start_node("52.50.92.141:10001", "52.51.101.205", 1, worker_path=test_path)
"""

import orchpy as op
import orchpy.services as services
import hyperparameter
import os
import time

op.connect("52.50.92.141:10001", "52.50.92.141:20001", "52.50.92.141:90899")

results = []
for stepsize in [1.0, 0.1, 0.01]:
  for momentum in [0.9, 0.5, 0.1]:
    results.append(hyperparameter.f([stepsize, momentum]))

for i in range(len(results)):
  op.pull(results[i])
