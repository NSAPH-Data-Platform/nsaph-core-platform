import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, \
    FIRST_COMPLETED, ALL_COMPLETED
from typing import Callable, Any


class BlockingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_queue_size:int, timeout=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_queue_size = max_queue_size
        self.tasks = dict()
        self.timeout = timeout
        self.log_timestamp = datetime.datetime.now()

    def submit(self, __fn: Callable, *args: Any, **kwargs: Any):
        self._wait(self.max_queue_size)
        task = super().submit(__fn, *args, **kwargs)
        self.tasks[task] = datetime.datetime.now()

    def wait(self):
        self._wait(0)
        return

    def _wait(self, n: int):
        while len(self.tasks) > n:
            # for task in as_completed(self.tasks):
            #     self.tasks.remove(task)
            if n > 0:
                w = FIRST_COMPLETED
            else:
                w = ALL_COMPLETED
            done, other = wait(self.tasks, timeout=self.timeout, return_when=w)
            for task in done:
                del self.tasks[task]
            t = datetime.datetime.now()
            if (t - self.log_timestamp) > datetime.timedelta(minutes=10):
                w, r = 0, 0
                for task in other:
                    if task.running():
                        t = datetime.datetime.now()
                        delta = self.tasks[task] - t
                        if delta > datetime.timedelta(hours=1):
                            logging.warning(
                                "Task {} has been in the queue for {}"
                                    .format(str(task), str(delta))
                            )
                        r += 1
                    else:
                        w += 1
                logging.debug(
                    "Tasks in the queue: running: {:d}; waiting: {:d}"
                        .format(r, w)
                )



