from datetime import timedelta
from CEP import CEP
from base.PatternStructure import PrimitiveEventStructure
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import EqCondition
from condition.Condition import Variable
from stream.FileStream import FileInputStream
from stream.FileStream import FileOutputStream
from plugin.cityBike.cityBike import CitiBikeCSVFormatter

# PERFORMANCE OPTIMIZATIONS for testing
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from tree.PatternMatchStorage import TreeStorageParameters
import time

from parallel.ParallelExecutionParameters import DataParallelExecutionParametersHirzelAlgorithm
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms


# Same 3-station pattern as before
threeStationPattern = Pattern(
    SeqOperator(
        PrimitiveEventStructure("BikeTrip", "a"),
        PrimitiveEventStructure("BikeTrip", "b"),
        PrimitiveEventStructure("BikeTrip", "c")
    ),
    AndCondition(
        # All must use the same bike_id
        EqCondition(
            Variable("a", lambda e: e["bike"]),
            Variable("b", lambda e: e["bike"])
        ),
        EqCondition(
            Variable("b", lambda e: e["bike"]),
            Variable("c", lambda e: e["bike"])
        ),
        # Contiguous trips: a.end == b.start
        EqCondition(
            Variable("a", lambda e: e["end"]),
            Variable("b", lambda e: e["start"])
        ),
        # b.end == c.start
        EqCondition(
            Variable("b", lambda e: e["end"]),
            Variable("c", lambda e: e["start"])
        )
    ),
    timedelta(hours=1)  # 1 hour window
)

# Optimized storage parameters
storage_params = TreeStorageParameters(
    sort_storage=False,
    clean_up_interval=1  # Most aggressive cleanup
)

eval_params = TreeBasedEvaluationMechanismParameters(
    storage_params=storage_params,
)

import multiprocessing
num_workers = 500 # max(10, multiprocessing.cpu_count() - 2)  # Use half of available cores

parallel_params = DataParallelExecutionParametersHirzelAlgorithm(
    platform=ParallelExecutionPlatforms.THREADING,
    units_number=num_workers,
    key="bike"  # Partition by bike_id
)


# Sequential execution for testing
cep = CEP([threeStationPattern], eval_params, parallel_params)

print("Testing with 10K events dataset...")
print("File: test_10k.csv (first 10,000 events)")

# Time the execution
start_time = time.time()

events = FileInputStream("test/EventFiles/test_10k.csv") # test/EventFiles/201801-citibike-tripdata_1_sorted.csv
cep.run(events, FileOutputStream("test/Matches", "test_10k_output.txt", console_output=False), CitiBikeCSVFormatter())

elapsed_time = time.time() - start_time

print(f"Completed in {elapsed_time:.2f} seconds")
print(f"Processing rate: {10000/elapsed_time:.0f} events/second")
print("Check test/Matches/test_10k_output.txt for matches")

# Count matches
try:
    with open("test/Matches/test_10k_output.txt", 'r') as f:
        trips_per_match = 3
        match_count = sum(1 for line in f if line.strip()) / trips_per_match
    print(f"Found {match_count} matches in 10K events")
except:
    print("Could not count matches")