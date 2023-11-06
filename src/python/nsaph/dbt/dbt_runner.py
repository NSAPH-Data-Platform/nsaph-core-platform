from typing import List

from nsaph.db import Connection
from nsaph.dbt.dbt_config import DBTConfig


class DBTRunner:
    def __init__(self, context: DBTConfig = None):
        if not context:
            context = DBTConfig(None, __doc__).instantiate()
        self.context = context
        self.scripts = self.context.script
        self.runs = 0
        self.successes = 0
        self.failures = 0

    def reset(self):
        self.runs = 0
        self.successes = 0
        self.failures = 0

    def run(self):
        with Connection(self.context.db, self.context.connection) as cnxn:
            for script_file in self.scripts:
                with open(script_file) as script:
                    self.run_script(script, cnxn)

    def run_script(self, script, cnxn):
        lines = [line for line in script]
        query = ''.join(lines)
        with cnxn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            pi = columns.index("passed")
            n = len(columns)
            rows = [row for row in cursor]
            lengths = [0 for _ in range(n)]
            passes = 0
            failures = 0
            test_cases = []
            for row in rows:
                values = [row[i] for i in range(n)]
                if row[pi]:
                    passes += 1
                    values[pi] = "passed"
                else:
                    failures += 1
                    values[pi] = "failed"
                for i in range(n):
                    if len(values[i]) > lengths[i]:
                        lengths[i] = len(str(values[i]))
                test_cases.append(values)
            lengths = [l + 1 for l in lengths]
            print(self.report_row(columns, lengths))
            for row in test_cases:
                s = self.report_row(row, lengths)
                print(s)
            print("Passed: {:d}; Failed: {:d}".format(passes, failures))
            self.runs      += len(test_cases)
            self.successes += passes
            self.failures  += failures
        return

    @classmethod
    def report_row(cls, row: List, lengths: List[int]) -> str:
        s = ""
        for i in range(len(lengths)):
            cell = str(row[i]).ljust(lengths[i]) + '\t'
            s += cell
        return s

    def test(self):
        self.reset()
        self.run()
        if self.failures > 0:
            raise AssertionError("There are failures")
        print("All tests succeeded")


if __name__ == '__main__':
    runner = DBTRunner()
    runner.test()
