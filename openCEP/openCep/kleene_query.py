#!/usr/bin/env python3
"""
Kleene Query - Configuration for Kleene closure pattern matching.
"""
from datetime import timedelta
from base.PatternStructure import PrimitiveEventStructure, SeqOperator, KleeneClosureOperator
from base.Pattern import Pattern
from condition.CompositeCondition import AndCondition, OrCondition
from condition.BaseRelationCondition import EqCondition, NotEqCondition
from condition.Condition import Variable
from condition.KCCondition import KCIndexCondition
from evaluation.EvaluationMechanismFactory import TreeBasedEvaluationMechanismParameters
from tree.PatternMatchStorage import TreeStorageParameters
from parallel.ParallelExecutionParameters import DataParallelExecutionParametersHirzelAlgorithm
from parallel.ParallelExecutionPlatforms import ParallelExecutionPlatforms

# Kleene closure query: 2-4 contiguous trips, ending at specific station
conditions = [
    # Same bike ID between last KC event and following event
    EqCondition(
        Variable("a", lambda a: a[-1]["bike"]),
        Variable("b", lambda e: e["bike"])
    ),
    # Contiguous trips: last KC event end = following event start  
    EqCondition(
        Variable("a", lambda a: a[-1]["end"]),
        Variable("b", lambda e: e["start"])
    ),
    # Filter for specific end stations
    OrCondition(
        EqCondition(Variable("b", lambda e: e["end"]), 252),
        EqCondition(Variable("b", lambda e: e["end"]), 264),
        EqCondition(Variable("b", lambda e: e["end"]), 3134),
    ),
    # KC constraint: same bike ID within KC events
    KCIndexCondition(
        names={'a'},
        getattr_func=lambda e: e["bike"],
        relation_op=lambda cur, prev: cur == prev,
        offset=-1
    ),
    # KC constraint: contiguous trips within KC events
    KCIndexCondition(
        names={'a'},
        getattr_func=lambda e: (e["start"], e["end"]),
        relation_op=lambda cur, prev: cur[0] == prev[1],
        offset=-1
    ),
    # Prevent duplicate event usage
    NotEqCondition(
        Variable("a", lambda a: a[-1]["started_at"]),
        Variable("b", lambda e: e["started_at"])
    )
]

kleenePattern = Pattern(
    SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a"), min_size=2, max_size=4),
        PrimitiveEventStructure("BikeTrip", "b"),
    ),
    AndCondition(*conditions),
    timedelta(hours=1)
)

# Setup evaluation parameters
eval_params = TreeBasedEvaluationMechanismParameters(
    storage_params=TreeStorageParameters(sort_storage=False, clean_up_interval=1)
)

parallel_params = DataParallelExecutionParametersHirzelAlgorithm(
    platform=ParallelExecutionPlatforms.THREADING,
    units_number=1000,
    key="bike"
)
