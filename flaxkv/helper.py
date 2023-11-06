import queue


class SimpleQueue:
    def __init__(self, maxsize: int):
        self.q = queue.Queue(maxsize=maxsize)

    def put(self, item):
        if not self.q.full():
            self.q.put(item)
        else:
            self.q.get()
            self.q.put(item)

    def get(self, block=True, timeout=None):
        return self.q.get(block=block, timeout=timeout)

    def empty(self):
        return self.q.empty()

    def clear(self):
        while not self.empty():
            self.get()
