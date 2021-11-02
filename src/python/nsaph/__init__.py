import datetime
import inspect
import logging
import sys
import os

NSAPH_LOG = False


def init_logging(with_thread_id = False, name = None):
    global NSAPH_LOG
    if NSAPH_LOG:
        return
    if not name:
        script = sys.argv[0]
        if script[0] == '-':
            script = None
            stack = inspect.stack()
            for frame in stack:
                if frame.frame.f_locals.get("__name__") == "__main__":
                    script = frame.filename
                    break
        if not script:
            return NSAPH_LOG
        script = os.path.basename(script)
        script = os.path.splitext(script)[0]
        name = script
    ts = datetime.datetime.now().isoformat(timespec="seconds", sep='-') \
            .replace(':', '-')
    file_name = "{}-{}.log".format(name, ts)
    if os.getenv("LOGDIR"):
        file_name = os.path.join(os.getenv("LOGDIR"), file_name)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(filename=file_name)
    ]
    for h in handlers:
        h.setLevel(logging.DEBUG)
        if with_thread_id:
            h.setFormatter(logging.Formatter("%(thread)d: %(message)s"))
        else:
            h.setFormatter(logging.Formatter("%(message)s"))
    logging.basicConfig(level=logging.DEBUG, handlers = handlers)
    NSAPH_LOG = True
    return NSAPH_LOG

# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler.setFormatter(formatter)
ORIGINAL_FILE_COLUMN = "FILE"