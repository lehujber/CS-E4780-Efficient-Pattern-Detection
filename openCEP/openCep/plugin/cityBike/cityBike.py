# plugin/cityBike/cityBike.py
import csv
from datetime import datetime
from typing import Any, Dict, Optional, Sequence
from base.DataFormatter import DataFormatter, EventTypeClassifier
import json

class CitiBikeEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):
        return "BikeTrip"

class CitiBikeCSVFormatter(DataFormatter):
    # Hard-code the header your file actually has
    HEADERS = [
        "tripduration","starttime","stoptime",
        "start station id","start station name","start station latitude","start station longitude",
        "end station id","end station name","end station latitude","end station longitude",
        "bikeid","usertype","birth year","gender"
    ]

    def __init__(self, event_type_classifier: EventTypeClassifier = CitiBikeEventTypeClassifier()):
        super().__init__(event_type_classifier)
        self._fieldnames: Sequence[str] = self.HEADERS  # no header autodetection

    def parse_event(self, raw_data: str):
        line = raw_data.strip()
        if not line:
            # return an inert event with epoch timestamp so the engine can ignore it
            return {"_raw": {}, "started_at": self._epoch(), "ended_at": self._epoch(), "bike": None, "start": None, "end": None}

        row = next(csv.reader([line]))
        # If the line is literally the header row, just ignore its values by mapping as data; it won’t match anyway.
        record = dict(zip(self._fieldnames, row + [""] * max(0, len(self._fieldnames) - len(row))))

        def to_int(x):
            try: return int(float(x))
            except: return None

        ev: Dict[str, Any] = {
            "bike": record.get("bikeid"),
            "start": to_int(record.get("start station id", "")),
            "end":   to_int(record.get("end station id", "")),
            "started_at": self._parse_dt(record.get("starttime", "")),
            "ended_at":   self._parse_dt(record.get("stoptime", "")),
            "_raw": record,
        }
        return ev

    def get_event_timestamp(self, event_payload: dict) -> datetime:
        ts = event_payload.get("ended_at") if event_payload else None
        return ts if isinstance(ts, datetime) else datetime.now()

    # --- helpers ---
    def _parse_dt(self, s: str) -> datetime:
        s = (s or "").strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)  # handles fractional seconds
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(s, fmt)
                    break
                except Exception:
                    dt = None
            if dt is None:
                dt = self._epoch()
        if dt.tzinfo is not None:
            dt = dt.astimezone(None).replace(tzinfo=None)
        return dt

    def _epoch(self) -> datetime:
        return datetime.fromtimestamp(0)

class DebugEventTypeClassifier(EventTypeClassifier):
    def get_event_type(self, event_payload: dict):
        return "Match"

class DebugFormatter(DataFormatter):
    """
    Formatter that dumps the entire match dict (all variables, including a-chain and b)
    as pretty JSON for debugging.
    """
    def __init__(self, event_type_classifier: EventTypeClassifier = DebugEventTypeClassifier()):
        super().__init__(event_type_classifier)

    # --- Input side (not really used here, but must return a dict) ---
    def parse_event(self, raw_data: str):
        # Just wrap the raw string in a dict so Event(...) doesn’t break
        #print("event processed")
        return {"raw": raw_data}

    def get_event_timestamp(self, event_payload: dict):
        return datetime.now()

    # --- Output side ---
    def format(self, event_payload: dict) -> str:
        return json.dumps(event_payload, default=str, indent=2)