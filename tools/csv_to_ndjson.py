import argparse
import ast
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Set


def smart_split(line: str) -> List[str]:
    """
    Split a CSV line by commas, but do NOT split commas inside:
      - double quotes
      - [...] arrays
      - {...} objects
    Note: assumes input is 1 record per physical line (no embedded newlines in quoted fields).
    """
    fields: List[str] = []
    buf: List[str] = []
    in_double = False
    esc = False
    depth_sq = 0
    depth_curly = 0

    for ch in line:
        if esc:
            buf.append(ch)
            esc = False
            continue

        if ch == "\\":
            buf.append(ch)
            esc = True
            continue

        if ch == '"':
            in_double = not in_double
            buf.append(ch)
            continue

        if not in_double:
            if ch == "[":
                depth_sq += 1
            elif ch == "]":
                depth_sq = max(0, depth_sq - 1)
            elif ch == "{":
                depth_curly += 1
            elif ch == "}":
                depth_curly = max(0, depth_curly - 1)

        if ch == "," and (not in_double) and depth_sq == 0 and depth_curly == 0:
            fields.append("".join(buf))
            buf = []
        else:
            buf.append(ch)

    fields.append("".join(buf))
    return fields


def render_bar(done: int, total: int, width: int = 30) -> str:
    if total <= 0:
        return "[" + "?" * width + "]"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(ratio * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def fmt_bytes(n: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024.0:
            return f"{f:.1f}{u}"
        f /= 1024.0
    return f"{f:.1f}PB"


def default_out_path(inp_path: str) -> str:
    base, _ = os.path.splitext(inp_path)
    return base + ".ndjson"


def strip_outer_quotes(s: str) -> str:
    """
    If the whole field is wrapped in double quotes, remove them and unescape "" -> "
    (common CSV behavior).
    """
    s2 = s.strip()
    if len(s2) >= 2 and s2[0] == '"' and s2[-1] == '"':
        inner = s2[1:-1]
        return inner.replace('""', '"')
    return s2


def parse_jsonish(value: str) -> Any:
    """
    Best-effort conversion for JSON-ish text:
      - "" / None / null -> None
      - JSON objects/arrays -> parsed via json.loads
      - Python-literal-ish (single quotes, True/False/None) -> ast.literal_eval
    If parsing fails, return original string.
    """
    if value is None:
        return None
    s = strip_outer_quotes(value).strip()
    if s == "" or s.lower() in {"none", "null"}:
        return None

    # First try JSON if it looks like JSON
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            pass

        # Then try python literal form
        try:
            return ast.literal_eval(s)
        except Exception:
            return s

    # Not a JSON-ish container; keep string
    return s


def main(
    inp: str,
    outp: str,
    sink_col: str = "__LAST__",
    json_cols: Optional[Set[str]] = None,
    empty_to_null: bool = True,
) -> None:
    total_size = os.path.getsize(inp)
    start = time.time()
    last_print = 0.0

    json_cols = json_cols or set()

    with open(inp, "r", encoding="utf-8", errors="replace", newline="") as f_in:
        header_line = f_in.readline()
        if not header_line:
            raise SystemExit("Empty input file")

        header_line = header_line.rstrip("\n").rstrip("\r")

        # A) safer header parsing
        header = [strip_outer_quotes(x) for x in smart_split(header_line)]
        expected = len(header)

        if sink_col == "__LAST__":
            sink_idx = expected - 1
        else:
            if sink_col not in header:
                raise SystemExit(f"Sink column '{sink_col}' not found in header")
            sink_idx = header.index(sink_col)

        bad_rows = 0
        rows = 0

        with open(outp, "w", encoding="utf-8", newline="") as f_out:
            line_no = 1
            while True:
                line = f_in.readline()
                if not line:
                    break
                line_no += 1

                line = line.rstrip("\n").rstrip("\r")
                if not line:
                    continue

                parts = smart_split(line)

                # Repair rows that have too many columns due to stray commas
                if len(parts) > expected:
                    overflow = parts[expected:]
                    parts = parts[:expected]
                    parts[sink_idx] = (parts[sink_idx] or "") + "," + ",".join(overflow)
                    bad_rows += 1
                elif len(parts) < expected:
                    parts += [""] * (expected - len(parts))
                    bad_rows += 1

                obj: Dict[str, Any] = {}
                for i in range(expected):
                    key = header[i]
                    raw_val = parts[i]

                    # Normalize empty -> null (optional)
                    if raw_val is None:
                        obj[key] = None
                        continue

                    v = raw_val.strip()
                    if empty_to_null and v == "":
                        obj[key] = None
                        continue

                    # B) parse certain columns into JSON types
                    if key in json_cols:
                        obj[key] = parse_jsonish(v)
                    else:
                        # Keep as string (but strip outer CSV quotes)
                        obj[key] = strip_outer_quotes(v)

                f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                rows += 1

                now = time.time()
                if now - last_print >= 0.2:
                    pos = f_in.tell()  # OK with readline()-style loop
                    bar = render_bar(pos, total_size)
                    elapsed = now - start
                    speed = pos / elapsed if elapsed > 0 else 0.0
                    pct = (pos / total_size * 100.0) if total_size > 0 else 0.0
                    sys.stdout.write(
                        f"\r{bar} {pct:6.2f}% {fmt_bytes(pos)}/{fmt_bytes(total_size)} "
                        f"{fmt_bytes(speed)}/s rows:{rows:,} repaired:{bad_rows:,}"
                    )
                    sys.stdout.flush()
                    last_print = now

        sys.stdout.write("\n")
        print(f"Done. Wrote NDJSON: {outp}")
        print(f"Rows: {rows:,} | repaired/padded: {bad_rows:,}")


def parse_args():
    ap = argparse.ArgumentParser(description="Convert CSV -> NDJSON with repair + JSON-column parsing.")
    ap.add_argument("input_csv", help="Input CSV path")
    ap.add_argument(
        "--sink-col",
        default="__LAST__",
        help="Column to absorb overflow when a row has extra commas. Default: __LAST__",
    )
    ap.add_argument(
        "--json-cols",
        default="",
        help="Comma-separated column names to parse as JSON-ish (arrays/objects). Example: itunes_apps,android_apps",
    )
    ap.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output NDJSON path (default: same base name with .ndjson)",
    )
    ap.add_argument(
        "--keep-empty-strings",
        action="store_true",
        help="If set, empty strings stay as '' instead of null",
    )
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    inp = args.input_csv
    outp = args.output or default_out_path(inp)

    json_cols = {c.strip() for c in args.json_cols.split(",") if c.strip()}
    empty_to_null = not args.keep_empty_strings

    main(inp, outp, sink_col=args.sink_col, json_cols=json_cols, empty_to_null=empty_to_null)
