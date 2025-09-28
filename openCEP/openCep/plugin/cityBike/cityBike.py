# plugin/citibike/CitiBike.py
import csv
from datetime import datetime
from typing import Any, Dict, Optional, Sequence

from base.DataFormatter import DataFormatter, EventTypeClassifier

PROBABILITY_KEY = "Probability"  # optional, only if you include it in your CSV


class CitiBikeEventTypeClassifier(EventTypeClassifier):
    """All records are 'BikeTrip' events for your patterns."""
    def get_event_type(self, event_payload: dict):
        return "BikeTrip"


class CitiBikeCSVFormatter(DataFormatter):
    """
    CSV -> event dict for Citi Bike data.
    - Works with headered CSVs by default (Citi Bike format).
    - Exposes canonical keys used by your pattern: 'bike', 'start', 'end'.
    - Uses 'ended_at' (configurable) as event time for ordering/windowing.
    """
    def __init__(
        self,
        event_type_classifier: EventTypeClassifier = CitiBikeEventTypeClassifier(),
        *,
        headers: Optional[Sequence[str]] = None,   
        has_header: bool = True,                   
        timestamp_field: str = "ended_at",         
        bike_field_candidates: Sequence[str] = ("bike_id", "bikeid", "vehicle_id", "bike", "bicycle_id"),
        start_station_candidates: Sequence[str] = ("start_station_id", "start_station", "start station id"),
        end_station_candidates: Sequence[str] = ("end_station_id", "end_station", "end station id"),
    ):
        super().__init__(event_type_classifier)
        self.headers = list(headers) if headers is not None else None
        self.has_header = has_header
        self.timestamp_field = timestamp_field
        self.bike_field_candidates = tuple(k.lower() for k in bike_field_candidates)
        self.start_station_candidates = tuple(k.lower() for k in start_station_candidates)
        self.end_station_candidates = tuple(k.lower() for k in end_station_candidates)

        self._fieldnames: Optional[Sequence[str]] = None  # discovered or provided

    def parse_event(self, raw_data: str):
        print("Parsing event...")
        line = raw_data.strip()
        if not line:
            return None

        row = next(csv.reader([line]))
        # First line: learn fieldnames
        if self._fieldnames is None:
            if self.headers is not None:
                self._fieldnames = list(self.headers)
            elif self.has_header and any(tok.lower() in ("ride_id", "started_at", "start_station_id", "end_station_id") for tok in row):
                # this line IS the header -> store & skip it
                self._fieldnames = [h.strip() for h in row]
                return None
            else:
                # Fallback order if no header is present (typical Citi Bike schema)
                self._fieldnames = [
                    "ride_id","rideable_type","started_at","ended_at",
                    "start_station_name","start_station_id",
                    "end_station_name","end_station_id",
                    "start_lat","start_lng","end_lat","end_lng",
                    "member_casual"
                ]

        # Length guard (rare CSV quirks)
        if len(row) < len(self._fieldnames):
            row += [""] * (len(self._fieldnames) - len(row))
        elif len(row) > len(self._fieldnames):
            row = row[:len(self._fieldnames)]

        record = dict(zip(self._fieldnames, row))

        bike = self._get_first(record, self.bike_field_candidates)
        start = self._get_first(record, self.start_station_candidates)
        end   = self._get_first(record, self.end_station_candidates)

        def maybe_int(x):
            try:
                return int(float(x))
            except Exception:
                return None if x in ("", None) else x

        ev: Dict[str, Any] = {
            # canonical fields used by your pattern
            "bike": bike,                           # IMPORTANT: must be a stable per-bike id
            "start": maybe_int(start),
            "end": maybe_int(end),
            # keep original row if you need more attributes later
            "_raw": record,
        }

        # preserve original timestamps; CEP will ask us for the event time via get_event_timestamp()
        for k in ("started_at", "ended_at"):
            if k in record and record[k]:
                ev[k] = self._parse_dt(record[k])

        return ev

    def get_event_timestamp(self, event_payload: dict):
        # Prefer configured field; fall back sensibly
        v = event_payload.get(self.timestamp_field) or event_payload.get("ended_at") or event_payload.get("started_at")
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            return self._parse_dt(v)
        # last resort
        return datetime.now()

    def get_probability(self, event_payload: Dict[str, Any]) -> Optional[float]:
        p = event_payload.get(PROBABILITY_KEY) or event_payload.get(PROBABILITY_KEY.lower(), None)
        try:
            return float(p) if p not in (None, "") else None
        except Exception:
            return None

    # ---------- helpers ----------
    def _parse_dt(self, s: str) -> datetime:
        s = s.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)  # handles "YYYY-MM-DD HH:MM:SS[.fff][±HH:MM]"
        except Exception:
            try:
                dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            except Exception:
                dt = datetime.fromtimestamp(0)
        # Make naive (OpenCEP often compares naive datetimes)
        if dt.tzinfo is not None:
            dt = dt.astimezone(None).replace(tzinfo=None)
        return dt

    def _get_first(self, record: Dict[str, Any], candidates: Sequence[str]):
        lower = {k.lower(): v for k, v in record.items()}
        for c in candidates:
            if c in lower:
                return lower[c]
        return None
