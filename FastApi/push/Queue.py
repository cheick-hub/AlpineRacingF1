import threading
import time

from collections import defaultdict
from queue import PriorityQueue, Empty
from utils import singleton


class PriorityQueueElement():
    def __init__(self, priority, second_prio,  task) -> None:
        self.priority = priority
        self.second_priority = second_prio
        self.task = task

    def __lt__(self, other):
        """
            "Lower than". Needed for the insertion in the PriorityQueue.
        """
        if self.priority == other.priority:
            return self.second_priority < other.second_priority
        return self.priority < other.priority


class PushQueue(metaclass=singleton._Singleton):
    def __init__(self, maxsize=0):
        # no KeyError when getting a key not in the dict
        self.second_priorities = defaultdict(lambda: 0)
        self.tasks_queue = PriorityQueue(maxsize=maxsize)
        self.worker_thread = threading.Thread(target=self.worker)
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def add_task(self, priority, task):
        """"
        Add a task to the queue. Block execution if queue is full.
        Possible to make it without block with put_nowait but raise Full exception.

        Priority (given as an exemple) : 
        - 1 (metadata) => return UIDs to the workers
        - 2 (other) => using secondary priority to keep them in the received order
        """
        if priority is None:
            raise ValueError('Task priority should be given.')

        self.second_priorities[priority] += 1
        self.tasks_queue.put(PriorityQueueElement(
            priority, self.second_priorities[priority], task))   # block=True

    def worker(self):
        """
        Infinite loop, always looking for tasks to do in the queue.
        Possible to block the thread with get() instead of get_nowait() and catching the error.
        """
        while True:
            try:
                item = self.tasks_queue.get_nowait()
                item.task()
                self.tasks_queue.task_done()
            except Empty:
                time_out = 5
                self.second_priorities = defaultdict(
                    lambda: 0)   # reset counters
                # print(f'Tasks queue empty : retrying in {time_out}sec')
                time.sleep(time_out)   # blocking, on porpuse
                continue
