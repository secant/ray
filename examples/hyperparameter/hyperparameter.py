import orchpy
import time

@orchpy.distributed([List[float]], [float])
def f(params):
  time.sleep(np.random.randint(10))
  return 0
