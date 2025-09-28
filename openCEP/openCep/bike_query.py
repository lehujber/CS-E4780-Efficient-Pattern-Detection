from datetime import timedelta
from CEP import CEP
# plan / base / condition / stream / plugin imports:
from base.PatternStructure import PrimitiveEventStructure
from base.Pattern import Pattern
from base.PatternStructure import SeqOperator
from condition.CompositeCondition import AndCondition
from condition.BaseRelationCondition import SmallerThanCondition
from condition.Condition import Variable
from stream.FileStream import FileInputStream
from stream.FileStream import FileOutputStream
from plugin.cityBike.cityBike import CitiBikeCSVFormatter

from base.PatternStructure import KleeneClosureOperator  # + add this
from condition.Condition import SimpleCondition   

# Example pattern from README: GOOG price strictly increasing within 3 minutes
from condition.KCCondition import KCIndexCondition   # <— new import
from condition.BaseRelationCondition import BinaryCondition
from condition.Condition import SimpleCondition, Variable

hotPathsPattern = Pattern(
    SeqOperator(
        KleeneClosureOperator(PrimitiveEventStructure("BikeTrip", "a")),
        PrimitiveEventStructure("BikeTrip", "b"),
    ),
    AndCondition(
        # 1) same bike across the a[] chain: a[i].bike == a[i-1].bike
        KCIndexCondition(
            names={'a'},
            getattr_func=lambda e: e["bike"],
            relation_op=lambda cur, prev: cur == prev,
            offset=-1
        ),
        # 2) station continuity across the chain: a[i].start == a[i-1].end
        KCIndexCondition(
            names={'a'},
            getattr_func=lambda e: (e["start"], e["end"]),
            relation_op=lambda cur, prev: cur[0] == prev[1],
            offset=-1
        ),
        # 3) last(a).bike == b.bike - FIXED: added relation_op parameter
        BinaryCondition(
            Variable("a", lambda e: e["bike"]),
            Variable("b", lambda e: e["bike"]),
            relation_op=lambda a_bike, b_bike: a_bike == b_bike
        ),
        # 4) b ends at a hot station - FIXED: use integers instead of floats
        SimpleCondition(
            Variable("b", lambda b: b["end"]),
            relation_op=lambda end_station: end_station in {72} 
        )
    ),
    timedelta(hours=1)
)


if __name__ == "__main__":
    cep = CEP([hotPathsPattern])
    # Use included demo data and write matches to test/Matches/output.txt
    events = FileInputStream("test/EventFiles/201801-citibike-tripdata.csv")
    cep.run(events, FileOutputStream("test/Matches", "output.txt"), CitiBikeCSVFormatter())

    print("Done. See test/Matches/output.txt")
