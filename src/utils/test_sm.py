from shared_memory_dict import SharedMemoryDict
from multiprocessing import resource_tracker

smd = SharedMemoryDict(name='vcc-test', size=1024)

smd['api_port'] = 11111
smd['rmq_port'] = 22222


resource_tracker.unregister(smd.shm._name, 'shared_memory')

smd.shm.close()

