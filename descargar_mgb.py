# https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries?filterId=Mod_Hydro_Output_Selected&startForecastTime=2026-01-27T00%3A00%3A00Z&endForecastTime=2026-01-28T00%3A00%3A00Z&documentFormat=PI_JSON

from datetime import datetime, timedelta
import requests
from typing import TypedDict, List
import json
import sys

# startForecastTime = "2026-01-27T00%3A00%3A00Z"
# endForecastTime = "2026-01-28T00%3A00%3A00Z"
documentFormat = "PI_JSON"
config_path = "config/config.json"

def loadConfig(config_path : str) -> dict:
    try:
        with open(config_path,"r",encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: no se encontró el archivo de configuración: {config_path}", file=sys.stderr)
        sys.exit(1)

    except json.JSONDecodeError as e:
        print(f"Error: JSON inválido en {config_path}: {e}", file=sys.stderr)
        sys.exit(2)

    except Exception as e:
        print(f"Error al inentar leer {config_path}: {e}", file=sys.stderr)
        sys.exit(3) 

    if "base_url" not in config:
        raise ValueError("Falta base_url en config")

    return config

config = loadConfig(config_path)

class Event(TypedDict):
    date : str
    time : str
    value : str
    flag : str

class TimeStep(TypedDict):
    unit : str
    multiplier : str

class DateTime(TypedDict):
    date : str
    time : str

class TimeSeriesHeader(TypedDict):
    type : str
    moduleInstanceId : str
    parameterId : str
    timeStep : TimeStep
    startDate : DateTime
    endDate : DateTime
    forecastDate : DateTime
    missVal : str
    stationName : str
    lat : str
    lon : str
    x : str
    y : str
    z : str
    units : str
    creationDate : str
    creationTime : str
    approvedDate : DateTime

class TimeSeries(TypedDict):
    header : TimeSeriesHeader
    events : List[Event]

class getTimeseriesResponse(TypedDict):
    version : str
    timeZone : str
    timeseries : List[TimeSeries]

def descargarMgb(
        fecha_pronostico : datetime = datetime.now(),
        filterId : str = config["default_filterId"] if "default_filterId" in config else None
) -> getTimeseriesResponse:
    inicio = datetime(fecha_pronostico.year, fecha_pronostico.month, fecha_pronostico.day)
    fin = inicio + timedelta(days=1)
    startForecastTime = "%sZ" % (inicio.isoformat(timespec='seconds'))
    endForecastTime =  "%sZ" % (fin.isoformat(timespec='seconds'))
    url = "%s/timeseries" % (config["base_url"])
    response = requests.get(
        url, 
        {
            "filterId": filterId, 
            "startForecastTime": startForecastTime, 
            "endForecastTime": endForecastTime, 
            "documentFormat": documentFormat
        }
    )
    if response.status_code >= 400:
        raise Exception("Falló la descarga: %s" % (response.text))
    return response.json()

if __name__ == "__main__":
    data = descargarMgb()
    json.dump(data, sys.stdout, indent=2)
    sys.stdout.write("\n")
