import pandas as pd
from app.accessor import Timeseries, download_timeseries
from datetime import datetime, timezone

# Importa simulado y observado de estaciones en mapping_file y guarda emparejado en .csv (1 archivo por estación)  

### PARAMS
forecast_date = datetime(2026,2,25,3,0,0,tzinfo=timezone.utc)
timestart = datetime(2025,1,1)
timeend = datetime(2026,12,31)
mapping_file = "static/mgb_map.csv"
sim_filterId = "Mod_Hydro_Output_Selected"
obs_filterId = "Tablero_Hydro"
import_obs = True
import_sim = True
output_dir = "data/paired"
###

df = pd.read_csv(open(mapping_file))

if import_sim:
    sim_data = download_timeseries(fecha_pronostico=forecast_date,filterId=sim_filterId, parameterIds=["Q.sim"], timestart = timestart, timeend=timeend)
    if "timeSeries" not in sim_data:
        raise ValueError("No se encontraron timeseries sim")
    sim_ts = Timeseries.from_api_response(sim_data, save=True)

for i, row in df.iterrows():
    print("Estación %s" % row["obs"])
    try:
        if import_obs:
            data = download_timeseries(filterId=obs_filterId, locationIds = [row["obs"]], parameterIds=["Q.obs"], timestart= timestart, timeend= timeend)
            ts = Timeseries.from_api_response(data, save=True)
        df_paired = Timeseries.read_paired(
            {"locationId": row["obs"], "parameterId": "Q.obs"},
            {"locationId": str(row["sim"]), "parameterId": "Q.sim", "forecastDate": forecast_date}
        )
        paired_filename = "%s/%s-%s-%s-%s.csv" % (output_dir, row["obs"], row["name"].replace(" ","")[0:12], str(row["sim"]), forecast_date.isoformat()[0:13])
        df_paired.to_csv(open(paired_filename, "w"), index=False)
    except Exception as e:
        print(str(e))
        continue

