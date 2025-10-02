import pandas as pd 
from src.integrity import check_duplicates, check_ranges 

def test_check_duplicates():
    df = pd.DataFrame({
        "timestamp": ["t1","t1","t2"],
        "equipment_id": ["E1","E1","E1"],
        "v": [1,2,3]
    })
    dups = check_duplicates(df, ["timestamp","equipment_id"])
    # Only one duplicate key pair should be reported
    assert len(dups) == 1
    assert set(dups.columns) == {"timestamp","equipment_id"}

def test_check_ranges():
    df = pd.DataFrame({
        "throughput_tph": [100, -5, 1200],   # -5 and 1200 are out of (0,1000)
        "power_kw": [10, 20, 30]
    })
    issues = check_ranges(df, {"throughput_tph": (0, 1000)})
    # Expect 2 offending rows
    assert len(issues) == 2
    assert set(["_col","_lo","_hi","throughput_tph"]).issubset(set(issues.columns))