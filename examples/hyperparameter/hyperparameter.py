from typing import List

import orchpy
import time
import numpy as np

@orchpy.distributed([int], [float])
def f(duration):
  time.sleep(duration)
  return 0.0
