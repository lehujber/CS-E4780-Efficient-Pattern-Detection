"""
Simple load shedding: shed events probabilistically when latency is high.
"""
import csv
import random

# Finisher stations - always keep these
FINISHER_ENDS = {252, 264, 3134}

# Global state (simple and direct)
_prob_map = {}
_threshold = None
_active = False
_stats = {
    "total": 0, 
    "shed": 0, 
    "total_parsed": 0,
    "processed_while_active": 0,
}


def load_probabilities(csv_path="input_shedding/P_hots/p_hot.csv"):
    """Load probability data once at startup."""
    global _prob_map
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            _prob_map[int(row['start_id'])] = float(row['prob_yes'])
    print(f"[LoadShedding] Loaded {len(_prob_map)} probabilities")


def enable(threshold_ms):
    """Enable load shedding with given latency threshold."""
    global _threshold, _active
    _threshold = threshold_ms
    _active = False  # Will activate when latency exceeds threshold
    print(f"[LoadShedding] Enabled with threshold {threshold_ms}ms")


def check_latency(latency_ms):
    """Check if latency exceeds threshold and activate shedding."""
    global _active, _threshold
    if _threshold and latency_ms > _threshold and not _active:
        _active = True
        print(f"[LoadShedding] ACTIVATED (latency {latency_ms:.1f}ms > {_threshold}ms)")


def count_parsed_event():
    """Count all parsed events (call before type filtering)."""
    _stats["total_parsed"] += 1


def should_shed(event_payload):
    """Return True if event should be discarded."""
    # Always count events that pass type filter
    _stats["total"] += 1
    
    if not _active:
        return False
    
    # Track events processed while active
    _stats["processed_while_active"] += 1
    
    end_station = event_payload.get('end')
    if not end_station or end_station in FINISHER_ENDS:
        return False
    
    prob_yes = _prob_map.get(end_station, 0.0)
    if random.random() > prob_yes:  # Shed with probability (1 - prob_yes)
        _stats["shed"] += 1
        return True
    return False


def print_stats():
    """Print shedding statistics."""
    total_parsed = _stats["total_parsed"]
    total_processed = _stats["total"]
    shed = _stats["shed"]
    processed_active = _stats["processed_while_active"]
    
    print(f"\n[LoadShedding] Statistics:")
    print(f"  Total events parsed: {total_parsed}")
    print(f"  Events after type filter: {total_processed}")
    if total_processed > 0:
        kept = total_processed - shed
        shed_rate = (shed / total_processed) * 100
        shed_rate_active = (shed / processed_active) * 100
        print(f"  Events kept: {kept}")
        print(f"  Events shed: {shed} ({shed_rate:.1f}%)")
        print(f"  Events processed with active shedding: {processed_active}, shed: ({shed_rate_active:.1f}%)")