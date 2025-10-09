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

# PERFORMANCE: Disable optimizer and configure fast cleanup + parallel execution
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from tree.PatternMatchStorage import TreeStorageParameters
from adaptive.optimizer.OptimizerFactory import OptimizerParameters
from adaptive.optimizer.OptimizerTypes import OptimizerTypes
from parallel.ParallelExecutionParameters import DataParallelExecutionParametersHirzelAlgorithm
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms

# Simple 3-station path query - FASTER
# SASE: EVENT SEQ(Bike a, Bike b, Bike c)
# WHERE [bike_id] && a.end_station_id == b.start_station_id && 
#       b.end_station_id == c.start_station_id
# WITHIN 1h

threeStationPattern = Pattern(
    # Structure: Simple sequence of exactly 3 BikeTrip events
    SeqOperator(
        PrimitiveEventStructure("BikeTrip", "a"),
        PrimitiveEventStructure("BikeTrip", "b"),
        PrimitiveEventStructure("BikeTrip", "c")
    ),
    # Conditions
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
    timedelta(hours=1)
)

# CRITICAL PERFORMANCE FIX: Disable optimizer and increase cleanup frequency
storage_params = TreeStorageParameters(
    sort_storage=False,  # No sorting overhead
    clean_up_interval=5  # Clean up expired matches every 5 additions (default is 10)
)

eval_params = TreeBasedEvaluationMechanismParameters(
    storage_params=storage_params,
)

# PARALLEL EXECUTION: Partition by bike_id to process each bike independently
# This uses Hirzel's group-by-key algorithm with threading
import multiprocessing
num_workers = 1000 # max(2, multiprocessing.cpu_count() // 2)  # Use half of available cores

parallel_params = DataParallelExecutionParametersHirzelAlgorithm(
    platform=ParallelExecutionPlatforms.THREADING,
    units_number=num_workers,
    key="bike"  # Partition by bike_id
)

if __name__ == "__main__":
    cep = CEP([threeStationPattern], eval_params, parallel_params)

    # Use included demo data
    events = FileInputStream("test/EventFiles/201801-citibike-tripdata.csv")
    cep.run(events, FileOutputStream("test/Matches", "output.txt", console_output=True), CitiBikeCSVFormatter())

    print("Done. Check test/Matches/output.txt for matches")