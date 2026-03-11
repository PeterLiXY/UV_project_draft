from __future__ import annotations

import argparse
import csv
import io
import math
import re
import statistics
import zipfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable


NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
MISSING_TOKENS = {"", " ", ".", "..", ". .", "n.a.", "na", "null", "none"}
SUPPRESSED_TOKENS = {"n.p.", "np", "suppressed"}
YEAR_PATTERN = re.compile(r"^(19|20)\d{2}$")
INTEGER_PATTERN = re.compile(r"^-?\d+$")
FLOAT_PATTERN = re.compile(r"^-?\d+\.\d+$")
CODE_PATTERN = re.compile(r"^[A-Z]\d{2}(\.\d+)?$")


def normalise_text(value: str) -> str:
    return value.replace("\r", " ").replace("\n", " ").strip()


def excel_column_name(index: int) -> str:
    letters = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def parse_scalar(raw: str) -> str | int | float | None:
    if raw is None:
        return None
    text = normalise_text(str(raw))
    lowered = text.lower()
    if lowered in MISSING_TOKENS or text == "":
        return None
    if lowered in SUPPRESSED_TOKENS:
        return text
    if INTEGER_PATTERN.fullmatch(text):
        try:
            return int(text)
        except ValueError:
            return text
    if FLOAT_PATTERN.fullmatch(text):
        try:
            return float(text)
        except ValueError:
            return text
    return text


def value_kind(value: object) -> str:
    if value is None:
        return "missing"
    if isinstance(value, int):
        if 1900 <= value <= 2100:
            return "year"
        return "integer"
    if isinstance(value, float):
        return "float"
    text = str(value)
    lowered = text.lower()
    if lowered in SUPPRESSED_TOKENS:
        return "suppressed"
    if YEAR_PATTERN.fullmatch(text):
        return "year-text"
    if CODE_PATTERN.fullmatch(text):
        return "code"
    return "text"


def detect_non_standard_formats(values: list[object], column_name: str) -> list[str]:
    issues: list[str] = []
    texts = [str(v) for v in values if isinstance(v, str)]
    if not texts:
        return issues

    kind_counts = Counter(value_kind(v) for v in values if v is not None)
    if len(kind_counts) > 1:
        top_kinds = ", ".join(f"{kind}={count}" for kind, count in kind_counts.most_common())
        issues.append(f"mixed value types ({top_kinds})")

    odd_whitespace = [t for t in texts if t != t.strip()]
    if odd_whitespace:
        issues.append(f"leading/trailing whitespace in {len(odd_whitespace)} values")

    placeholder_count = sum(1 for t in texts if t.lower() in SUPPRESSED_TOKENS)
    if placeholder_count:
        issues.append(f"suppressed placeholder values in {placeholder_count} rows")

    unusual_examples = []
    for text in texts:
        if any(ord(ch) > 127 for ch in text):
            unusual_examples.append(text)
        if len(unusual_examples) == 3:
            break
    if unusual_examples:
        preview = "; ".join(repr(example) for example in unusual_examples)
        issues.append(f"non-ASCII text detected, sample: {preview}")

    if "age" in column_name.lower():
        non_standard_age = [
            text
            for text in texts
            if not re.fullmatch(r"\d{2}[-–]\d{2}|\d{2}\D+\d{2}|\d{2}\+|All ages", text)
        ]
        if non_standard_age:
            preview = ", ".join(repr(item) for item in non_standard_age[:3])
            issues.append(f"age group labels use inconsistent formatting, sample: {preview}")

    return issues


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot compute percentile of empty sequence")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def detect_outliers(values: list[object]) -> dict[str, object] | None:
    numeric = [float(v) for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if len(numeric) < 8:
        return None
    sorted_values = sorted(numeric)
    q1 = percentile(sorted_values, 0.25)
    q3 = percentile(sorted_values, 0.75)
    iqr = q3 - q1
    if iqr == 0:
        return None
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = [value for value in numeric if value < lower or value > upper]
    if not outliers:
        return None
    return {
        "count": len(outliers),
        "share": len(outliers) / len(numeric),
        "lower_bound": lower,
        "upper_bound": upper,
        "examples": [round(value, 3) for value in sorted(set(outliers))[:5]],
    }


def is_year_column(column_name: str, values: list[object]) -> bool:
    non_null = [value for value in values if value is not None]
    if not non_null:
        return False
    year_like = [value for value in non_null if isinstance(value, int) and 1900 <= value <= 2100]
    if "year" in column_name.casefold():
        return bool(year_like)
    return len(year_like) >= max(5, int(len(non_null) * 0.9))


def load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.findall("main:si", NS):
        parts = [node.text or "" for node in item.iterfind(".//main:t", NS)]
        strings.append("".join(parts))
    return strings


def workbook_sheet_targets(archive: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    sheets: list[tuple[str, str]] = []
    for sheet in workbook.find("main:sheets", NS):
        rid = sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        sheets.append((sheet.attrib["name"], "xl/" + rel_map[rid]))
    return sheets


def extract_sheet_rows(archive: zipfile.ZipFile, sheet_path: str, shared_strings: list[str]) -> list[list[object]]:
    sheet = ET.fromstring(archive.read(sheet_path))
    data = sheet.find("main:sheetData", NS)
    if data is None:
        return []

    rows: list[list[object]] = []
    for row in data.findall("main:row", NS):
        current: dict[int, object] = {}
        max_index = 0
        for cell in row.findall("main:c", NS):
            ref = cell.attrib.get("r", "")
            letters = "".join(ch for ch in ref if ch.isalpha())
            index = 0
            for ch in letters:
                index = index * 26 + (ord(ch.upper()) - 64)
            max_index = max(max_index, index)

            value_node = cell.find("main:v", NS)
            inline_node = cell.find("main:is/main:t", NS)
            if value_node is not None:
                raw = value_node.text or ""
            elif inline_node is not None:
                raw = inline_node.text or ""
            else:
                raw = ""

            if cell.attrib.get("t") == "s" and raw != "":
                raw = shared_strings[int(raw)]
            current[index] = parse_scalar(raw)

        if max_index == 0:
            continue
        rows.append([current.get(i) for i in range(1, max_index + 1)])
    return rows


def infer_header_row(rows: list[list[object]]) -> int:
    best_index = 0
    best_score = -1
    for idx, row in enumerate(rows[:20]):
        strings = [value for value in row if isinstance(value, str) and value not in SUPPRESSED_TOKENS]
        score = len(strings)
        joined = " ".join(strings).lower()
        if "year" in joined:
            score += 3
        if "data type" in joined:
            score += 3
        if "count" in joined:
            score += 2
        if score > best_score:
            best_index = idx
            best_score = score
    return best_index


def compact_column_name(name: object, index: int) -> str:
    text = normalise_text(str(name)) if name is not None else ""
    return text or f"Column {excel_column_name(index)}"


def prepare_table(rows: list[list[object]]) -> tuple[list[str], list[list[object]], int]:
    if not rows:
        return [], [], 0

    header_index = infer_header_row(rows)
    raw_header = rows[header_index]
    max_len = max(len(row) for row in rows)
    header = [compact_column_name(raw_header[i] if i < len(raw_header) else None, i + 1) for i in range(max_len)]

    data_rows: list[list[object]] = []
    for row in rows[header_index + 1 :]:
        padded = row + [None] * (max_len - len(row))
        if any(value is not None for value in padded):
            data_rows.append(padded[:max_len])

    return header, data_rows, header_index


def find_column_index(header: list[str], column_name: str) -> int | None:
    lowered = column_name.casefold()
    for index, name in enumerate(header):
        if name.casefold() == lowered:
            return index
    return None


def filter_data_rows(
    header: list[str],
    rows: list[list[object]],
    cancer_group_filter: str | None,
) -> list[list[object]]:
    if not cancer_group_filter:
        return rows

    lowered_filter = cancer_group_filter.casefold()
    column_index = find_column_index(header, "Cancer group/site")
    if column_index is None:
        return rows

    filtered = []
    for row in rows:
        value = row[column_index] if column_index < len(row) else None
        text = "" if value is None else str(value)
        if lowered_filter in text.casefold():
            filtered.append(row)
    return filtered


def profile_rows(dataset_name: str, rows: list[list[object]], cancer_group_filter: str | None = None) -> str:
    if not rows:
        return f"Dataset: {dataset_name}\nNo rows found.\n"

    header, data_rows, header_index = prepare_table(rows)
    data_rows = filter_data_rows(header, data_rows, cancer_group_filter)

    columns = {name: [row[idx] for row in data_rows] for idx, name in enumerate(header)}
    lines = [f"Dataset: {dataset_name}"]
    lines.append(f"Rows: {len(data_rows)}")
    lines.append(f"Columns: {len(header)}")
    lines.append(f"Header row: {header_index + 1}")
    if cancer_group_filter:
        lines.append(f"Cancer group/site filter: {cancer_group_filter}")

    if not data_rows:
        lines.append("No rows matched the filter.")
        return "\n".join(lines) + "\n"

    missing_summary = []
    for name, values in columns.items():
        missing = sum(value is None for value in values)
        if missing:
            missing_summary.append((name, missing, missing / len(values)))
    lines.append("Missing values:")
    if missing_summary:
        for name, count, share in sorted(missing_summary, key=lambda item: item[1], reverse=True)[:10]:
            lines.append(f"  - {name}: {count} ({share:.1%})")
    else:
        lines.append("  - none")

    lines.append("Non-standard formats:")
    non_standard_found = False
    for name, values in columns.items():
        issues = detect_non_standard_formats(values, name)
        if issues:
            non_standard_found = True
            lines.append(f"  - {name}: " + "; ".join(issues))
    if not non_standard_found:
        lines.append("  - none detected")

    lines.append("Outliers:")
    outlier_found = False
    for name, values in columns.items():
        outlier_info = detect_outliers(values)
        if outlier_info:
            outlier_found = True
            lines.append(
                "  - "
                f"{name}: {outlier_info['count']} values ({outlier_info['share']:.1%}), "
                f"bounds [{outlier_info['lower_bound']:.3f}, {outlier_info['upper_bound']:.3f}], "
                f"examples {outlier_info['examples']}"
            )
    if not outlier_found:
        lines.append("  - none detected")

    lines.append("Unique categories:")
    category_found = False
    for name, values in columns.items():
        non_null = [value for value in values if value is not None]
        if not non_null:
            continue
        if all(isinstance(value, str) for value in non_null):
            unique = sorted(set(str(value) for value in non_null))
            if len(unique) <= 20:
                category_found = True
                preview = ", ".join(unique[:10])
                more = "" if len(unique) <= 10 else f" ... (+{len(unique) - 10} more)"
                lines.append(f"  - {name}: {len(unique)} unique -> {preview}{more}")
    if not category_found:
        lines.append("  - no low-cardinality text columns")

    lines.append("Year distribution:")
    year_found = False
    for name, values in columns.items():
        if not is_year_column(name, values):
            continue
        years = [int(value) for value in values if isinstance(value, int) and 1900 <= value <= 2100]
        if years:
            year_found = True
            counts = Counter(years)
            top_years = ", ".join(f"{year}:{counts[year]}" for year in sorted(counts)[:10])
            lines.append(
                f"  - {name}: min={min(years)}, max={max(years)}, unique={len(counts)}, sample={top_years}"
            )
    if not year_found:
        lines.append("  - no year-like column found")

    return "\n".join(lines) + "\n"


def split_sql_tuples(values_block: str) -> list[str]:
    tuples: list[str] = []
    current: list[str] = []
    depth = 0
    in_string = False
    i = 0
    while i < len(values_block):
        ch = values_block[i]
        if ch == "'" and (i == 0 or values_block[i - 1] != "\\"):
            in_string = not in_string
        if not in_string:
            if ch == "(":
                depth += 1
            if depth > 0:
                current.append(ch)
            if ch == ")":
                depth -= 1
                if depth == 0:
                    tuples.append("".join(current))
                    current = []
        elif depth > 0:
            current.append(ch)
        i += 1
    return tuples


def parse_sql_value(token: str) -> object:
    text = token.strip()
    if text.upper() == "NULL":
        return None
    if text.startswith("'") and text.endswith("'"):
        return parse_scalar(text[1:-1].replace("\\'", "'"))
    return parse_scalar(text)


def load_sql_rows(path: Path) -> tuple[list[str], list[list[object]]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    create_match = re.search(r"CREATE TABLE\s+\w+\s*\((.*?)\)\s*ENGINE=", text, flags=re.S | re.I)
    if not create_match:
        raise ValueError(f"Could not find CREATE TABLE statement in {path.name}")

    columns: list[str] = []
    for raw_line in create_match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.upper().startswith(("PRIMARY KEY", "KEY", "UNIQUE", "CONSTRAINT")):
            continue
        columns.append(line.split()[0].strip("`"))

    rows: list[list[object]] = []
    for match in re.finditer(r"INSERT INTO .*? VALUES\s*(.*?);", text, flags=re.S | re.I):
        values_block = match.group(1)
        for tuple_text in split_sql_tuples(values_block):
            reader = csv.reader(io.StringIO(tuple_text[1:-1]), delimiter=",", quotechar="'", escapechar="\\")
            parsed = next(reader)
            rows.append([parse_sql_value(value) for value in parsed])
    return columns, rows


def profile_sql(path: Path, cancer_group_filter: str | None = None) -> str:
    columns, rows = load_sql_rows(path)
    padded_rows = [row + [None] * (len(columns) - len(row)) for row in rows]
    table_name = path.name + " [SQL]"
    content = profile_rows(table_name, [columns] + padded_rows, cancer_group_filter=cancer_group_filter)
    extra_lines = ["SQL-specific checks:"]

    if "postcode" in columns:
        idx = columns.index("postcode")
        postcodes = [str(row[idx]) for row in padded_rows if row[idx] is not None]
        non_four_digit = [value for value in postcodes if not re.fullmatch(r"\d{4}", value)]
        extra_lines.append(f"  - postcode values not 4 digits: {len(non_four_digit)}")

    if "latitude" in columns and "longitude" in columns:
        lat_idx = columns.index("latitude")
        lon_idx = columns.index("longitude")
        zero_coords = [
            row for row in padded_rows if row[lat_idx] == 0 and row[lon_idx] == 0
        ]
        extra_lines.append(f"  - rows with (0, 0) coordinates: {len(zero_coords)}")

    return content + "\n" + "\n".join(extra_lines) + "\n"


def profile_xlsx(path: Path, cancer_group_filter: str | None = None) -> str:
    sections: list[str] = []
    with zipfile.ZipFile(path) as archive:
        shared_strings = load_shared_strings(archive)
        for sheet_name, sheet_path in workbook_sheet_targets(archive):
            if "welcome" in sheet_name.lower():
                continue
            rows = extract_sheet_rows(archive, sheet_path, shared_strings)
            sections.append(
                profile_rows(
                    f"{path.name} [{sheet_name}]",
                    rows,
                    cancer_group_filter=cancer_group_filter,
                )
            )
    return "\n".join(sections)


def main() -> None:
    parser = argparse.ArgumentParser(description="Lightweight EDA for .xlsx and .sql files without external packages.")
    parser.add_argument("paths", nargs="*", help="Files to profile. Defaults to all .xlsx and .sql files in the current folder.")
    parser.add_argument(
        "--cancer-group",
        help="Case-insensitive substring filter applied to the 'Cancer group/site' column.",
    )
    args = parser.parse_args()

    if args.paths:
        paths = [Path(item) for item in args.paths]
    else:
        cwd = Path.cwd()
        paths = [
            path for path in sorted(cwd.glob("*.xlsx")) if not path.name.startswith("~$")
        ] + sorted(cwd.glob("*.sql"))

    if not paths:
        raise SystemExit("No .xlsx or .sql files found.")

    reports: list[str] = []
    for path in paths:
        suffix = path.suffix.lower()
        if suffix == ".xlsx":
            reports.append(profile_xlsx(path, cancer_group_filter=args.cancer_group))
        elif suffix == ".sql":
            reports.append(profile_sql(path, cancer_group_filter=args.cancer_group))
        else:
            reports.append(f"Skipped unsupported file: {path.name}\n")

    print("\n".join(reports))


if __name__ == "__main__":
    main()
