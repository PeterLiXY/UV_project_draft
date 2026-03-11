from __future__ import annotations

import argparse
import csv
import re
import zipfile
from pathlib import Path

import EDA


DEFAULT_FILTER = "Melanoma of the skin"
DEFAULT_OUTPUT_DIR = "filtered_datasets"
VALID_SEX_VALUES = {"Females", "Males", "Persons"}
NULL_TOKENS = {"", " ", ".", "..", ". .", "n.a.", "na", "null", "none", "n.p.", "np", "suppressed"}
YEAR_VALUE_PATTERN = re.compile(r"^(19|20)\d{2}$")
AGE_GROUP_CODE_MAP = {
    "00-04": 1,
    "05-09": 2,
    "10-14": 3,
    "15-19": 4,
    "20-24": 5,
    "25-29": 6,
    "30-34": 7,
    "35-39": 8,
    "40-44": 9,
    "45-49": 10,
    "50-54": 11,
    "55-59": 12,
    "60-64": 13,
    "65-69": 14,
    "70-74": 15,
    "75-79": 16,
    "80-84": 17,
    "85-89": 18,
    "90+": 19,
    "All ages combined": 20,
}


def slugify(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    return "_".join(part for part in cleaned.split("_") if part)


def filter_rows_exact(header: list[str], rows: list[list[object]], column_name: str, target_value: str) -> list[list[object]]:
    column_index = EDA.find_column_index(header, column_name)
    if column_index is None:
        return []

    filtered: list[list[object]] = []
    target = target_value.casefold()
    for row in rows:
        value = row[column_index] if column_index < len(row) else None
        if value is None:
            continue
        if str(value).strip().casefold() == target:
            filtered.append(row)
    return filtered


def normalise_missing_value(value: object) -> object:
    if value is None:
        return "NULL"
    text = str(value).strip()
    if text.casefold() in NULL_TOKENS:
        return "NULL"
    return value


def canonical_age_group_key(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if text.casefold() in NULL_TOKENS:
        return None
    if text == "All ages":
        return "All ages combined"
    if text == "All ages combined":
        return "All ages combined"
    if text == "90+":
        return "90+"

    digits = re.findall(r"\d+", text)
    if len(digits) >= 2:
        return f"{digits[0].zfill(2)}-{digits[1].zfill(2)}"
    return text


def age_group_code(value: object) -> object:
    key = canonical_age_group_key(value)
    if key is None:
        return "NULL"
    return AGE_GROUP_CODE_MAP.get(key, "NULL")


def is_valid_year(value: object) -> bool:
    if value is None:
        return False
    return bool(YEAR_VALUE_PATTERN.fullmatch(str(value).strip()))


def is_valid_sex(value: object) -> bool:
    if value is None:
        return False
    return str(value).strip() in VALID_SEX_VALUES


def has_cancer_group(value: object) -> bool:
    if value is None:
        return False
    return str(value).strip() != ""


def clean_filtered_rows(header: list[str], rows: list[list[object]]) -> list[list[object]]:
    year_index = EDA.find_column_index(header, "Year")
    sex_index = EDA.find_column_index(header, "Sex")
    cancer_group_index = EDA.find_column_index(header, "Cancer group/site")
    age_group_index = EDA.find_column_index(header, "Age group (years)")

    cleaned_rows: list[list[object]] = []
    for row in rows:
        if year_index is not None and not is_valid_year(row[year_index] if year_index < len(row) else None):
            continue
        if sex_index is not None and not is_valid_sex(row[sex_index] if sex_index < len(row) else None):
            continue
        if cancer_group_index is not None and not has_cancer_group(row[cancer_group_index] if cancer_group_index < len(row) else None):
            continue

        cleaned_row = [normalise_missing_value(value) for value in row]
        if age_group_index is not None:
            cleaned_row.append(age_group_code(row[age_group_index] if age_group_index < len(row) else None))
        cleaned_rows.append(cleaned_row)

    return cleaned_rows


def workbook_rows(path: Path) -> list[tuple[str, list[str], list[list[object]]]]:
    datasets: list[tuple[str, list[str], list[list[object]]]] = []
    with zipfile.ZipFile(path) as archive:
        shared_strings = EDA.load_shared_strings(archive)
        for sheet_name, sheet_path in EDA.workbook_sheet_targets(archive):
            if "welcome" in sheet_name.lower():
                continue
            rows = EDA.extract_sheet_rows(archive, sheet_path, shared_strings)
            header, data_rows, _ = EDA.prepare_table(rows)
            datasets.append((sheet_name, header, data_rows))
    return datasets


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in rows:
            writer.writerow("" if value is None else value for value in row)


def export_filtered_dataset(source_path: Path, output_dir: Path, cancer_group: str) -> tuple[Path, int]:
    workbook_name = source_path.stem
    all_rows: list[list[object]] = []
    header_for_output: list[str] | None = None

    for _sheet_name, header, data_rows in workbook_rows(source_path):
        filtered_rows = filter_rows_exact(header, data_rows, "Cancer group/site", cancer_group)
        if not filtered_rows:
            continue
        if header_for_output is None:
            age_group_index = EDA.find_column_index(header, "Age group (years)")
            header_for_output = header + (["Age group code"] if age_group_index is not None else [])
        all_rows.extend(clean_filtered_rows(header, filtered_rows))

    if header_for_output is None:
        raise ValueError(f"'Cancer group/site' column or matching rows not found in {source_path.name}")

    output_name = f"{workbook_name}__{slugify(cancer_group)}.csv"
    output_path = output_dir / output_name
    write_csv(output_path, header_for_output, all_rows)
    return output_path, len(all_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Formal data wrangling for AIHW cancer workbooks with a strict Cancer group/site filter."
    )
    parser.add_argument(
        "--cancer-group",
        default=DEFAULT_FILTER,
        help="Exact value to keep from the 'Cancer group/site' column. Default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for the filtered CSV datasets. Default: %(default)s",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Workbook files to wrangle. Defaults to all .xlsx files in the current folder.",
    )
    args = parser.parse_args()

    if args.paths:
        workbook_paths = [Path(item) for item in args.paths]
    else:
        workbook_paths = [path for path in sorted(Path.cwd().glob("*.xlsx")) if not path.name.startswith("~$")]

    if not workbook_paths:
        raise SystemExit("No .xlsx files found.")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[str] = []
    for workbook_path in workbook_paths:
        output_path, row_count = export_filtered_dataset(workbook_path, output_dir, args.cancer_group)
        summaries.append(f"{workbook_path.name} -> {output_path.name} ({row_count} rows)")

    print("Filtered datasets created:")
    for summary in summaries:
        print(f"  - {summary}")


if __name__ == "__main__":
    main()
