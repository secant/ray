import orchpy as op
import orchpy.services as services
import hyperparameter
import os
import time

results = []
for stepsize in [1.0, 0.1, 0.01]:
  for momentum in [0.9, 0.5, 0.1]:
    results.append(hyperparameter.f([stepsize, momentum]))

for i in range(len(results)):
  orchpy.pull(results[i])
