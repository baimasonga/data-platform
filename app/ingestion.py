"""File parsing boundary for the local ingestion service."""
import csv
import io
import json
import uuid

import duckdb
import pandas as pd

MAX_ROWS = 100_000


def load_frame(content, suffix, base):
    source = io.BytesIO(content)
    if suffix == ".csv":
        # Validate all records before inference: pandas otherwise treats excess
        # fields as an index, silently removing them when registering the frame.
        reader = csv.reader(io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader, [])
        if not header or any(not x.strip() for x in header) or len(set(header)) != len(header):
            raise ValueError("CSV headers must be unique and non-empty.")
        if len(header) > 200:
            raise ValueError("Import requires 1–200 columns.")
        count = 0
        for row in reader:
            if not row:  # Match pandas' handling of empty physical lines.
                continue
            if len(row) != len(header):
                raise ValueError(f"CSV record ending on line {reader.line_num} has {len(row)} fields; expected {len(header)}.")
            count += 1
            if count > MAX_ROWS:
                raise ValueError("Import requires 1–100,000 rows.")
        # Nullable integers avoid float coercion when an integer column has gaps.
        return pd.read_csv(source, nrows=MAX_ROWS + 1, dtype_backend="numpy_nullable")
    if suffix == ".json":
        obj = json.loads(content)
        if not isinstance(obj, list) or not obj or not all(isinstance(r, dict) for r in obj):
            raise ValueError("JSON must be a non-empty array of row objects.")
        return pd.DataFrame(obj, dtype=object)
    if suffix == ".xlsx":
        # Read incrementally; reject workbooks with excessive expanded content.
        import zipfile
        with zipfile.ZipFile(source) as archive:
            if sum(f.file_size for f in archive.infolist()) > 100 * 1024 * 1024:
                raise ValueError("Expanded workbook exceeds 100 MB.")
        source.seek(0)
        from openpyxl import load_workbook
        book = load_workbook(source, read_only=True, data_only=True)
        try:
            rows = book.worksheets[0].iter_rows(values_only=True)
            header = next(rows, ())
            if not header or any(v is None for v in header) or len(set(header)) != len(header):
                raise ValueError("Excel headers must be unique and non-empty.")
            import itertools
            return pd.DataFrame(itertools.islice(rows, MAX_ROWS + 1), columns=header, dtype=object)
        finally:
            book.close()
    if suffix == ".parquet":
        # DuckDB provides the reader, avoiding a separate Arrow dependency.
        path = base / f"import_{uuid.uuid4().hex}.parquet"
        try:
            path.write_bytes(content)
            with duckdb.connect(config={"memory_limit": "256MB", "threads": "2"}) as con:
                return con.execute("SELECT * FROM read_parquet(?) LIMIT ?", [str(path), MAX_ROWS + 1]).df()
        finally:
            path.unlink(missing_ok=True)
    raise ValueError("Supported files: CSV, JSON, XLSX and Parquet.")
