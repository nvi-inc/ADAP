from shared_memory_dict import SharedMemoryDict
from multiprocessing import resource_tracker

smd = SharedMemoryDict(name='vcc-test', size=1024)

print(smd.get('api_port', 0))
print(smd.get('rmq_port', 0))

resource_tracker.unregister(smd.shm._name, 'shared_memory')
smd.shm.close()
