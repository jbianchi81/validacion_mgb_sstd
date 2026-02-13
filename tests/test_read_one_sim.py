from app.accessor import Timeseries
from datetime import datetime, timedelta, timezone

def test_read_one_sim():
    ts = Timeseries.read_one(
        locationId = "5862", 
        parameterId = "Q.sim", 
        forecastDate = datetime(2026,2,13,3,0,0,tzinfo=timezone.utc)
    )
    assert(ts is not None)