from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


DEFAULT_INPUT = (
    "filtered_datasets/"
    "aihw-can-122-CDiA-2023-Book-7-Cancer-incidence-by-state-and-territory__melanoma_of_the_skin.csv"
)
DEFAULT_OUTPUT = "filtered_datasets/territory_melanoma_rate_by_year.svg"
DEFAULT_RATE_COLUMN = "Age-standardised rate 2023 Australian population  (per 100,000)"
DEFAULT_SEX = "Persons"
COLORS = [
    "#1f77b4",
    "#d62728",
    "#2ca02c",
    "#ff7f0e",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#17becf",
]


def load_series(path: Path, sex: str, rate_column: str) -> dict[str, list[tuple[int, float]]]:
    series: dict[str, list[tuple[int, float]]] = defaultdict(list)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("Sex") != sex:
                continue
            year = row.get("Year", "").strip()
            territory = row.get("State or Territory", "").strip()
            rate = row.get(rate_column, "").strip()
            if not year or not territory or not rate or rate == "NULL":
                continue
            series[territory].append((int(year), float(rate)))

    for territory in series:
        series[territory].sort(key=lambda item: item[0])
    return dict(sorted(series.items()))


def scale(value: float, low: float, high: float, start: float, end: float) -> float:
    if high == low:
        return (start + end) / 2
    return start + (value - low) * (end - start) / (high - low)


def svg_line_chart(series: dict[str, list[tuple[int, float]]], title: str, y_label: str) -> str:
    width = 1100
    height = 700
    left = 90
    right = 240
    top = 70
    bottom = 80
    plot_width = width - left - right
    plot_height = height - top - bottom

    years = sorted({year for points in series.values() for year, _ in points})
    rates = [rate for points in series.values() for _, rate in points]
    min_year, max_year = min(years), max(years)
    min_rate = 0.0
    max_rate = max(rates)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#faf8f3" />',
        f'<text x="{left}" y="34" font-family="Arial" font-size="24" font-weight="bold" fill="#1f1f1f">{title}</text>',
        f'<text x="{left}" y="58" font-family="Arial" font-size="13" fill="#555">x-axis: Year | y-axis: Age-standardised melanoma rate</text>',
        f'<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#333" stroke-width="1.5" />',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#333" stroke-width="1.5" />',
    ]

    y_ticks = 6
    for i in range(y_ticks + 1):
        value = min_rate + (max_rate - min_rate) * i / y_ticks
        y = scale(value, min_rate, max_rate, top + plot_height, top)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_width}" y2="{y:.1f}" stroke="#ddd" stroke-width="1" />')
        parts.append(
            f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" font-family="Arial" font-size="12" fill="#555">{value:.0f}</text>'
        )

    year_step = max(1, len(years) // 8)
    tick_years = years[::year_step]
    if tick_years[-1] != years[-1]:
        tick_years.append(years[-1])
    for year in tick_years:
        x = scale(year, min_year, max_year, left, left + plot_width)
        parts.append(f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + plot_height}" stroke="#eee" stroke-width="1" />')
        parts.append(
            f'<text x="{x:.1f}" y="{top + plot_height + 24}" text-anchor="middle" font-family="Arial" font-size="12" fill="#555">{year}</text>'
        )

    parts.append(
        f'<text x="{left + plot_width / 2:.1f}" y="{height - 20}" text-anchor="middle" font-family="Arial" font-size="14" fill="#333">Year</text>'
    )
    parts.append(
        f'<text x="24" y="{top + plot_height / 2:.1f}" transform="rotate(-90 24 {top + plot_height / 2:.1f})" text-anchor="middle" font-family="Arial" font-size="14" fill="#333">{y_label}</text>'
    )

    for idx, (territory, points) in enumerate(series.items()):
        color = COLORS[idx % len(COLORS)]
        coords = []
        for year, rate in points:
            x = scale(year, min_year, max_year, left, left + plot_width)
            y = scale(rate, min_rate, max_rate, top + plot_height, top)
            coords.append((x, y))
        path = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
        parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{path}" />')
        for x, y in coords:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.5" fill="{color}" />')

    legend_x = left + plot_width + 20
    legend_y = top + 10
    for idx, territory in enumerate(series):
        color = COLORS[idx % len(COLORS)]
        y = legend_y + idx * 26
        parts.append(f'<line x1="{legend_x}" y1="{y}" x2="{legend_x + 24}" y2="{y}" stroke="{color}" stroke-width="3" />')
        parts.append(
            f'<text x="{legend_x + 32}" y="{y + 4}" font-family="Arial" font-size="13" fill="#333">{territory}</text>'
        )

    parts.append("</svg>")
    return "\n".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prototype territory melanoma line chart.")
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to the filtered territory CSV.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output SVG path.")
    parser.add_argument("--sex", default=DEFAULT_SEX, help="Sex category to plot. Default: %(default)s")
    parser.add_argument("--rate-column", default=DEFAULT_RATE_COLUMN, help="Rate column to plot.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    series = load_series(input_path, sex=args.sex, rate_column=args.rate_column)
    if not series:
        raise SystemExit("No data found for the selected options.")

    svg = svg_line_chart(
        series,
        title=f"Melanoma incidence by state or territory ({args.sex})",
        y_label="Age-standardised rate per 100,000",
    )
    output_path.write_text(svg, encoding="utf-8")
    print(f"Saved chart to {output_path}")


if __name__ == "__main__":
    main()
