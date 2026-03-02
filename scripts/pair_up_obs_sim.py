import pandas as pd
from app.accessor import Timeseries, download_timeseries
from datetime import datetime, timezone, timedelta
import argparse
from pathlib import Path

# Importa simulado y observado de estaciones en mapping_file y guarda emparejado en .csv (1 archivo por estación)  

def parse_date(value: str):
    # Accept YYYY-MM-DD
    return datetime.strptime(value, "%Y-%m-%d")


def parse_datetime_utc(value: str):
    # Accept YYYY-MM-DDTHH:MM
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except Exception:
        dt = datetime.strptime(value, "%Y-%m-%d")
    return dt

## default forecast date
fd = (datetime.now() - timedelta(hours=13)).replace(hour=0, minute=0, second=0, microsecond=0)

### DEFAULT PARAMS
default_params = {
"forecast_date": fd,
"timestart": fd - timedelta(weeks=52),
"timeend":  fd + timedelta(weeks=26),
"mapping_file": "static/mgb_map.csv",
"sim_filterId": "Mod_Hydro_Output_Selected",
"obs_filterId":  "Tablero_Hydro",
"import_obs":  True,
"import_sim": True,
"output_dir": "data/paired"
}
###

def run(args):

    df = pd.read_csv(open(args.mapping_file))

    if args.import_sim:
        sim_data = download_timeseries(fecha_pronostico=args.forecast_date,filterId=args.sim_filterId, parameterIds=["Q.sim"], timestart = args.timestart, timeend=args.timeend)
        if "timeSeries" not in sim_data:
            raise ValueError("No se encontraron timeseries sim")
        sim_ts = Timeseries.from_api_response(sim_data, save=True)

    for i, row in df.iterrows():
        print("Estación %s" % row["obs"])
        try:
            if args.import_obs:
                data = download_timeseries(filterId=args.obs_filterId, locationIds = [row["obs"]], parameterIds=["Q.obs"], timestart= args.timestart, timeend= args.timeend)
                ts = Timeseries.from_api_response(data, save=True)
            df_paired = Timeseries.read_paired(
                {"locationId": row["obs"], "parameterId": "Q.obs"},
                {"locationId": str(row["sim"]), "parameterId": "Q.sim", "forecastDate": args.forecast_date}
            )

            Path(args.output_dir).mkdir(parents=True, exist_ok=True)
            paired_filename = "%s/%s-%s-%s-%s.csv" % (args.output_dir, row["obs"], row["name"].replace(" ","")[0:12], str(row["sim"]), args.forecast_date.isoformat()[0:13])
            df_paired.to_csv(open(paired_filename, "w"), index=False)
        except Exception as e:
            print(str(e))
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Importa simulado y observado de estaciones en mapping_file y guarda emparejado en .csv (1 archivo por estación)")

    # --- DATETIME PARAMS ---

    parser.add_argument(
        "--forecast-date",
        type=parse_datetime_utc,
        default=default_params["forecast_date"],
        help="Forecast datetime in UTC (format: YYYY-MM-DDTHH:MM)",
    )

    parser.add_argument(
        "--timestart",
        type=parse_date,
        default=default_params["timestart"],
        help="Start date (format: YYYY-MM-DD)",
    )

    parser.add_argument(
        "--timeend",
        type=parse_date,
        default=default_params["timeend"],
        help="End date (format: YYYY-MM-DD)",
    )

    # --- FILES / FILTERS ---

    parser.add_argument(
        "--mapping-file",
        default=default_params["mapping_file"],
    )

    parser.add_argument(
        "--sim-filterId",
        default=default_params["sim_filterId"],
    )

    parser.add_argument(
        "--obs-filterId",
        default=default_params["obs_filterId"],
    )

    # --- FLAGS ---

    parser.add_argument(
        "--import-obs",
        action=argparse.BooleanOptionalAction,
        default=default_params["import_obs"],
        help="Enable/disable observation import",
    )

    parser.add_argument(
        "--import-sim",
        action=argparse.BooleanOptionalAction,
        default=default_params["import_sim"],
        help="Enable/disable simulation import",
    )

    # --- OUTPUT ---

    parser.add_argument(
        "--output-dir",
        default=default_params["output_dir"],
    )

    args = parser.parse_args()

    print(args)

    run(args)