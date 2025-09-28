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

from condition.Condition import SimpleCondition, Variable
from datetime import timedelta
from base.PatternStructure import PrimitiveEventStructure, SeqOperator
from base.Pattern import Pattern

# Guaranteed-to-match: single BikeTrip with a trivially true condition
matchAnyTripPattern = Pattern(
    SeqOperator(
        PrimitiveEventStructure("BikeTrip", "a")
    ),
    SimpleCondition(
        Variable("a", lambda _: True),   # ignore the event, just return True
        relation_op=lambda _: True
    ),
    timedelta(hours=1)                  # window is irrelevant for 1-event patterns
)

cep = CEP([matchAnyTripPattern])

# Use included demo data and write matches to test/Matches/output.txt
if __name__ == "__main__":
    events = FileInputStream("test/EventFiles/201401-citibike-tripdata_1.csv")
    cep.run(events, FileOutputStream("test/Matches", "output.txt"), CitiBikeCSVFormatter())

    print("Done. See test/Matches/output.txt")
