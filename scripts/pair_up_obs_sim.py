import pandas as pd
from app.accessor import Timeseries, download_timeseries
from datetime import datetime, timezone

### PARAMS
forecast_date = datetime(2026,2,24,3,0,0,tzinfo=timezone.utc)
timestart = datetime(2025,1,1)
timeend = datetime(2026,12,31)
mapping_file = "data/mgb_validation_mapping.csv"
import_obs = False
import_sim = False
###

df = pd.read_csv(open(mapping_file))

if import_sim:
    sim_data = download_timeseries(fecha_pronostico=forecast_date,filterId="Mod_Hydro_Output_Selected", parameterIds=["Q.obs"], timestart = timestart, timeend=timeend)
    sim_ts = Timeseries.from_api_response(sim_data, save=True)

for i, row in df.iterrows():
    print("Estación %s" % row["obs"])
    try:
        if import_obs:
            data = download_timeseries(filterId="Tablero_Hydro", locationIds = [row["obs"]], parameterIds=["Q.obs"], timestart= timestart, timeend= timeend)
            ts = Timeseries.from_api_response(data, save=True)
        df_paired = Timeseries.read_paired(
            {"locationId": row["obs"], "parameterId": "Q.obs"},
            {"locationId": str(row["sim"]), "parameterId": "Q.sim", "forecastDate": forecast_date}
        )
        paired_filename = "data/paired/%s-%s-%s-%s.csv" % (row["obs"], row["name"].replace(" ","")[0:12], str(row["sim"]), forecast_date.isoformat()[0:13])
        df_paired.to_csv(open(paired_filename, "w"), index=False)
    except Exception as e:
        print(str(e))
        continue

