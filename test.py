
import threading
from queue import Queue
import time
import gc
logfile = 'access.log-20150920'

lock = threading.Lock()
q = Queue()
ar = []
def do_work(item):
    #a = item.split()
    #ar.append(a)
    pass
    #with lock:
    #    #print(threading.current_thread().name, item)
    #    pass

def worker():
    while True:
        item = q.get()
        do_work(item)
        q.task_done()

for i in range(1):
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()

x = 0
start = time.perf_counter()

lines = [line.rstrip('\n').split() for line in open(logfile, 'r', encoding='latin-1')]
for line in lines:
    do_work(line)

#with open(logfile, 'r', encoding='latin-1') as infile:
#    for l in infile:
#        #q.put(x)
#        do_work(l)
#        x += 1
q.join()
print('time:',time.perf_counter()-start)
