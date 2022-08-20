#  Copyright (c) 2021. Harvard University
#
#  Developed by Research Software Engineering,
#  Faculty of Arts and Sciences, Research Computing (FAS RC)
#  Author: Michael A Bouzinier
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import datetime
import inspect
import logging
import sys
import os

NSAPH_LOG = False


def app_name() -> str:
    """
    Constructs application name. This application name can
    be used to identify the application to the database process
    or in the log file

    :return: A string, containing application name
    """

    script = sys.argv[0]
    if script[0] == '-':
        script = None
        stack = inspect.stack()
        for frame in stack:
            if frame.frame.f_locals.get("__name__") == "__main__":
                script = frame.filename
                break
    if not script:
        return "nsaph"
    script = os.path.basename(script)
    script = os.path.splitext(script)[0]
    return script


def init_logging(with_thread_id = False, name = None, level = logging.DEBUG):
    """
    Initializes logging. By default, log is written to a file and copied to
    the standard output

    :param with_thread_id: if True, then thread numerical id is added
        to the log
    :param name: name of the application to be used by logger. By default,
        the name would be constructed by :func:app_name
    :param level: Log level, default is DEBUG
    :return:
    """

    global NSAPH_LOG, NSAPH_APP_NAME
    if NSAPH_LOG:
        return
    if not name:
        name = app_name()
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
        h.setLevel(level)
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