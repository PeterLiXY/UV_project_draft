from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from datetime import datetime
from statistics import mean
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.parse import urlencode
from urllib.request import urlopen


DEFAULT_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_HOURLY = ["uv_index", "cloud_cover", "temperature_2m", "relative_humidity_2m"]
DEFAULT_DAILY = ["sunrise", "sunset", "uv_index_max", "temperature_2m_max", "temperature_2m_min"]


def should_send_api_key(api_url: str) -> bool:
    hostname = (urlparse(api_url).hostname or "").lower()
    return hostname.startswith("customer-")


def fetch_open_meteo_data(
    latitude: float,
    longitude: float,
    timezone: str,
    forecast_days: int,
    hourly: list[str],
    daily: list[str],
) -> dict:
    api_url = os.getenv("OPEN_METEO_URL", DEFAULT_URL)
    api_key = os.getenv("OPEN_METEO_API_KEY")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(hourly),
        "daily": ",".join(daily),
        "forecast_days": forecast_days,
        "timezone": timezone,
    }

    # Open-Meteo only expects apikey on commercial customer-* hosts.
    if api_key and should_send_api_key(api_url):
        params["apikey"] = api_key

    request_url = f"{api_url}?{urlencode(params)}"
    try:
        with urlopen(request_url, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            "Open-Meteo request failed with "
            f"HTTP {exc.code}. URL: {request_url}\nResponse: {details}"
        ) from exc


def parse_iso_timestamps(values: list[str]) -> list[datetime]:
    timestamps = []
    for value in values:
        try:
            timestamps.append(datetime.fromisoformat(value))
        except ValueError:
            continue
    return timestamps


def numeric_summary(name: str, values: list[float | int]) -> str:
    if not values:
        return f"  - {name}: no numeric data"
    return (
        f"  - {name}: count={len(values)}, min={min(values):.2f}, "
        f"max={max(values):.2f}, mean={mean(values):.2f}"
    )


def category_summary(name: str, values: list[str]) -> str:
    counts = Counter(values)
    top_items = ", ".join(f"{key}:{counts[key]}" for key in counts.most_common(5))
    return f"  - {name}: unique={len(counts)}, top={top_items}"


def uv_risk_bucket(value: float) -> str:
    if value >= 11:
        return "Extreme"
    if value >= 8:
        return "Very High"
    if value >= 6:
        return "High"
    if value >= 3:
        return "Moderate"
    return "Low"


def cloud_bucket(value: float) -> str:
    if value >= 80:
        return "Very Cloudy"
    if value >= 50:
        return "Cloudy"
    if value >= 20:
        return "Partly Cloudy"
    return "Clear"


def explore_hourly(hourly_data: dict) -> list[str]:
    lines = ["Hourly exploration:"]
    timestamps = parse_iso_timestamps(hourly_data.get("time", []))
    if timestamps:
        lines.append(
            f"  - time coverage: {timestamps[0].isoformat()} to {timestamps[-1].isoformat()} "
            f"({len(timestamps)} hourly records)"
        )
        hour_counts = Counter(ts.hour for ts in timestamps)
        hour_preview = ", ".join(f"{hour:02d}:00={hour_counts[hour]}" for hour in sorted(hour_counts)[:6])
        lines.append(f"  - hour distribution sample: {hour_preview}")
    else:
        lines.append("  - time coverage: unavailable")

    for key, values in hourly_data.items():
        if key == "time":
            continue
        numeric_values = [value for value in values if isinstance(value, (int, float))]
        lines.append(numeric_summary(key, numeric_values))

    if "uv_index" in hourly_data:
        uv_categories = [uv_risk_bucket(float(value)) for value in hourly_data["uv_index"] if isinstance(value, (int, float))]
        lines.append(category_summary("uv_risk", uv_categories))

    if "cloud_cover" in hourly_data:
        sky_categories = [cloud_bucket(float(value)) for value in hourly_data["cloud_cover"] if isinstance(value, (int, float))]
        lines.append(category_summary("cloud_cover_label", sky_categories))

    return lines


def explore_daily(daily_data: dict) -> list[str]:
    lines = ["Daily exploration:"]
    timestamps = parse_iso_timestamps(daily_data.get("time", []))
    if timestamps:
        lines.append(
            f"  - day coverage: {timestamps[0].date().isoformat()} to {timestamps[-1].date().isoformat()} "
            f"({len(timestamps)} daily records)"
        )
    else:
        lines.append("  - day coverage: unavailable")

    if "sunrise" in daily_data and "sunset" in daily_data:
        sunrise = parse_iso_timestamps(daily_data["sunrise"])
        sunset = parse_iso_timestamps(daily_data["sunset"])
        daylight_hours = [
            (set_time - rise_time).total_seconds() / 3600
            for rise_time, set_time in zip(sunrise, sunset)
        ]
        lines.append(numeric_summary("daylight_hours", daylight_hours))

    for key, values in daily_data.items():
        if key in {"time", "sunrise", "sunset"}:
            continue
        numeric_values = [value for value in values if isinstance(value, (int, float))]
        lines.append(numeric_summary(key, numeric_values))

    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Exploratory analysis for Open-Meteo forecast data.")
    parser.add_argument("--latitude", type=float, default=-37.8136)
    parser.add_argument("--longitude", type=float, default=144.9631)
    parser.add_argument("--timezone", default="Australia/Melbourne")
    parser.add_argument("--forecast-days", type=int, default=3)
    parser.add_argument("--hourly", nargs="+", default=DEFAULT_HOURLY)
    parser.add_argument("--daily", nargs="+", default=DEFAULT_DAILY)
    args = parser.parse_args()

    data = fetch_open_meteo_data(
        latitude=args.latitude,
        longitude=args.longitude,
        timezone=args.timezone,
        forecast_days=args.forecast_days,
        hourly=args.hourly,
        daily=args.daily,
    )

    lines = [
        "Open-Meteo exploratory analysis",
        f"Latitude: {data.get('latitude')}",
        f"Longitude: {data.get('longitude')}",
        f"Timezone: {data.get('timezone')}",
        f"Timezone abbreviation: {data.get('timezone_abbreviation')}",
        f"Elevation: {data.get('elevation')}",
        f"Requested hourly variables: {', '.join(args.hourly)}",
        f"Requested daily variables: {', '.join(args.daily)}",
    ]

    hourly_data = data.get("hourly", {})
    daily_data = data.get("daily", {})
    lines.extend(explore_hourly(hourly_data))
    lines.extend(explore_daily(daily_data))

    print("\n".join(lines))


if __name__ == "__main__":
    main()
