from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Any


class BlockingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_queue_size:int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_queue_size = max_queue_size
        self.tasks = set()

    def submit(self, __fn: Callable, *args: Any, **kwargs: Any):
        while len(self.tasks) >= self.max_queue_size:
            for task in as_completed(self.tasks):
                self.tasks.remove(task)
        task = super().submit(__fn, *args, **kwargs)
        self.tasks.add(task)

    def wait(self):
        while len(self.tasks) > 0:
            for task in as_completed(self.tasks):
                self.tasks.remove(task)
        return

