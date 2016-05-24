from typing import List

import orchpy
import time
import numpy as np

@orchpy.distributed([List[float]], [float])
def f(params):
  time.sleep(np.random.randint(10))
  return 0
