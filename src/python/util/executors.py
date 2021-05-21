import datetime
import logging
#import pydevd
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, \
    FIRST_COMPLETED, ALL_COMPLETED
from typing import Callable, Any


def thread_initializer():
    pass
    #pydevd.settrace(suspend=False, trace_only_current_thread=True)


class BlockingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_queue_size:int, timeout=None, *args, **kwargs):
        super().__init__(initializer=thread_initializer, *args, **kwargs)
        self.max_queue_size = max_queue_size
        self.tasks = dict()
        self.timeout = timeout
        self.log_timestamp = datetime.datetime.now()

    def submit(self, __fn: Callable, *args: Any, **kwargs: Any):
        self.wait(self.max_queue_size)
        task = super().submit(__fn, *args, **kwargs)
        self.tasks[task] = datetime.datetime.now()

    def wait_for_completion(self):
        self._log()
        self.wait(0)
        return

    def _wait(self, return_when):
        t0 = datetime.datetime.now()
        t = t0
        while (t - t0).total_seconds() < self.timeout:
            try:
                done, other = wait(self.tasks, timeout=300, return_when=return_when)
                return done, other
            except TimeoutError as x:
                logging.warning("Long wait on running threads".format(str(datetime.datetime.now())))
                self._log()
        raise TimeoutError

    def _log(self):
        w, r, d = 0, 0, 0
        for task in self.tasks:
            if task.running():
                t = datetime.datetime.now()
                delta = t - self.tasks[task]
                if delta > datetime.timedelta(hours=1):
                    logging.warning(
                        "Task {} has been in the queue for {}"
                            .format(str(task), str(delta))
                    )
                r += 1
            elif task.done():
                d += 1
            else:
                w += 1
        logging.debug(
            "{}. Tasks in the queue: done: {:d}; running: {:d}; waiting: {:d}"
                .format(str(datetime.datetime.now()), d, r, w)
        )

    def wait(self, n: int):
        while len(self.tasks) > n:
            # for task in as_completed(self.tasks):
            #     self.tasks.remove(task)
            if n > 0:
                w = FIRST_COMPLETED
            else:
                w = ALL_COMPLETED
            done, other = self._wait(return_when=w)
            for task in done:
                del self.tasks[task]
            t = datetime.datetime.now()
            if (t - self.log_timestamp) > datetime.timedelta(minutes=10):
                self._log()



