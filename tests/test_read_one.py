from app.accessor import Timeseries
from datetime import datetime, timedelta, timezone

def test_read_one():
    ts = Timeseries.read_one(
        locationId= "AR_INA_8_INA_24_Q",
        parameterId= "Q.obs"
    )
    assert(ts is not None)