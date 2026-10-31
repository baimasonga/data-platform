"""Untrusted code executes ONLY inside the disposable, offline runner container."""
import ast
from contextlib import redirect_stdout, redirect_stderr
from datetime import date, datetime
from decimal import Decimal
import io
import json
import math
import sys
import traceback

import numpy as np
import pandas as pd


class OutputLimit(Exception):
    pass


class Capture(io.StringIO):
    def write(self, text):
        if self.tell() + len(text) > 20000:
            raise OutputLimit('Printed output exceeds 20,000 characters.')
        return super().write(text)


def scalar(value):
    if isinstance(value, np.generic):
        value = value.item()
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) > 9007199254740991:
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)[:10000]


def execute(payload):
    datasets = {item['id']: pd.DataFrame(item['rows'], columns=item['columns'], dtype=object)
                for item in payload['datasets']}
    namespace = {'__name__': '__notebook__', 'pd': pd, 'np': np, 'datasets': datasets}
    results, total = [], 0
    for cell in payload['cells']:
        captured, displays = Capture(), []

        def display(value):
            nonlocal total
            if len(displays) >= 8:
                raise OutputLimit('A cell can display at most eight outputs.')
            if isinstance(value, pd.Series):
                value = value.to_frame()
            if isinstance(value, pd.DataFrame):
                output = {'kind': 'table', 'columns': [str(c)[:500] for c in value.columns[:50]],
                          'rows': [[scalar(c) for c in row] for row in
                                   value.iloc[:1000, :50].itertuples(index=False, name=None)],
                          'truncated': len(value) > 1000 or len(value.columns) > 50}
            else:
                output = {'kind': 'text', 'text': str(value)[:20000]}
            total += len(json.dumps(output).encode())
            if total > 1000000:
                raise OutputLimit('Displayed output exceeds the 1 MB run limit.')
            displays.append(output)

        namespace['display'] = display
        result = {'index': cell['index'], 'stdout': '', 'outputs': displays, 'error': None}
        try:
            with redirect_stdout(captured), redirect_stderr(captured):
                tree = ast.parse(cell['text'], filename=f"<cell {cell['index'] + 1}>")
                expression = tree.body.pop() if tree.body and isinstance(tree.body[-1], ast.Expr) else None
                exec(compile(tree, f"<cell {cell['index'] + 1}>", 'exec'), namespace)
                if expression:
                    value = eval(compile(ast.Expression(expression.value), f"<cell {cell['index'] + 1}>", 'eval'), namespace)
                    if value is not None:
                        display(value)
        except BaseException as exc:
            result['error'] = ''.join(traceback.format_exception_only(type(exc), exc))[:2000]
        result['stdout'] = captured.getvalue()
        results.append(result)
        if result['error']:
            break
    return {'cells': results}


if __name__ == '__main__':
    payload = json.loads(sys.stdin.buffer.read(16000001))
    result = execute(payload)
    sys.stdout.write(json.dumps(result, allow_nan=False))
