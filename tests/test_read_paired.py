from app.accessor import Timeseries
from datetime import datetime, timedelta, timezone

def test_read_paired():
    df = Timeseries.read_paired(
        {"locationId": "AR_INA_8_INA_24_Q", "parameterId": "Q.obs"},
        {"locationId": "5862", "parameterId": "Q.sim", "forecastDate": datetime(2026,2,13,3,0,0,tzinfo=timezone.utc)},
        # timestart : datetime | None = None,
        # timeend : datetime | None = None,
        # obs_flag : str | None = None,
        # sim_flag : str | None = None
    )
    assert(df is not None)