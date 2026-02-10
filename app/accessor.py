# https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries?filterId=Mod_Hydro_Output_Selected&startForecastTime=2026-01-27T00%3A00%3A00Z&endForecastTime=2026-01-28T00%3A00%3A00Z&documentFormat=PI_JSON

from datetime import datetime, timedelta, timezone
import requests
from typing import TypedDict, List, Self, Tuple
import json
import sys
from dataclasses import dataclass
import logging
from .utils import loadConfig, execStmt, execStmtMany
import psycopg
from psycopg import sql
from textwrap import dedent

logger = logging.getLogger(__name__)

# startForecastTime = "2026-01-27T00%3A00%3A00Z"
# endForecastTime = "2026-01-28T00%3A00%3A00Z"
documentFormat = "PI_JSON"
config_path = "config/config.json"

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
    locationId : str
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

class TimeseriesResponse(TypedDict):
    header : TimeSeriesHeader
    events : List[Event]

class GetTimeseriesResponse(TypedDict):
    version : str
    timeZone : str
    timeseries : List[TimeseriesResponse]

@dataclass
class Location(TypedDict):
    locationId : str
    stationName : str
    lat : str
    lon : str
    x : str
    y : str
    z : str

    @classmethod
    def from_api_response(cls, data : TimeseriesResponse):
        # def parseLocation(data : TimeseriesResponse) -> Location:
        if "header" not in data:
            raise ValueError("Falta el header")
        return cls(
            locationId = data["header"]["locationId"],
            stationName = data["header"]["stationName"],
            lat = float(data["header"]["lat"]),
            lon = float(data["header"]["lon"]),
            x = float(data["header"]["x"]),
            y = float(data["header"]["y"]),
            z = float(data["header"]["z"])
        )


    def create(self) -> str:
        id = execStmt(
            config["user_dsn"],
            dedent("""
                INSERT INTO location (id, station_name, geometry) 
                VALUES (
                    %s,
                    %s,
                    ST_SetSrid(
                        ST_point(
                            %s,
                            %s
                        ),
                        4326
                    )
                )
                ON CONFLICT (id) 
                    DO UPDATE SET 
                        station_name=excluded.station_name, 
                        geometry=excluded.geometry 
                RETURNING id
            """),
            (self.locationId, self.stationName, self.lon, self.lat))
        return id

@dataclass
class TimeseriesValue(TypedDict):
    id : int | None
    timeseries_id : int | None
    time : datetime
    value : float
    flag : int
    comment : str | None

    @classmethod
    def parse_one(cls, event : Event, time_zone : float=0.0):
    # def parseValue(event : Event, time_zone : float=0.0) -> TimeseriesValue:
        return cls(
            time = parseDateTime(event["date"], event["time"], time_zone),
            value = float(event["value"]),
            flag = int(event["flag"])
        )

    @classmethod
    def from_api_response(cls, data : TimeseriesResponse, time_zone : float=0.0):
    # def parseValues(data : TimeseriesResponse, time_zone : float=0.0) -> List[TimeseriesValue]:
        if "events" not in data:
            raise ValueError("Falta 'events'")
        return [ cls.parse_one(event, time_zone) for event in data["events"] ]

    def to_row(self):
        return (self.timeseries_id, self.time, self.value, self.flag, self.comment)

    create_stmt = """
        INSERT INTO timeseries_values (series_id, time, value, flag, comment) 
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (series_id, time) 
            DO UPDATE SET 
                value=excluded.value, 
                flag=excluded.flag,
                comment=excluded.comment
        RETURNING id
    """

    @classmethod
    def create_many(cls, values : List[Self]):
        rows = [ v.to_row() for v in values ]
        return execStmtMany(
            config["user_dsn"],
            cls.create_stmt,
            rows
        )
        
    def create(self) -> str:
        id = execStmt(
            config["user_dsn"],
            dedent(self.create_stmt),
            self.to_row())
        self.id = id
        return id

@dataclass
class Timeseries(TypedDict):
    id : int | None
    locationId : str
    parameterId : str
    qualifierId : str | None
    timestep : timedelta
    units : str
    # timeZone : str
    forecastDate : datetime | None
    # metadata : dict
    location : Location
    values : List[TimeseriesValue]

    @classmethod
    def from_api_response(cls, data : GetTimeseriesResponse):
        time_zone = float(data["timeZone"])
        parsed = [Timeseries.parse_one(d, time_zone) for d in data["timeseries"]]
        return parsed
    
    @classmethod
    def parse_and_create(cls, data : GetTimeseriesResponse):
        ts_items = cls.from_api_response(data)
        created_ids = cls.create_many(ts_items)
        if len(ts_items) > len(created_ids):
            logging.warning("Algunos elementos no se crearon (%i > %i)" % (len(ts_items), len(created_ids)))
        return ts_items

    @classmethod
    def parse_and_create_one(cls, data : TimeseriesResponse, time_zone : float=0.0):
        timeseries = cls.parse_one(data, time_zone)
        timseries_id = timeseries.create_all()
        return timeseries

    @classmethod
    def parse_one(cls, data : TimeseriesResponse, time_zone : float=0.0):
    # def parseTimeseries(data : TimeseriesResponse, time_zone : float=0.0) -> Timeseries:
        if "header" not in data:
            raise ValueError("Falta el header")
        return cls(
            locationId = data["header"]["locationId"],
            parameterId = data["header"]["parameterId"],
            qualifierId = data["header"]["qualifierId"],
            forecastDate = parseDateTime(data["header"]["forecastDate"]["date"], data["header"]["forecastDate"]["time"], time_zone),
            timestep = parseTimestep(data["header"]["timestep"]),
            units = data["header"]["units"],
            location = Location.from_api_response(data),
            values = TimeseriesValue.from_api_response(data, time_zone)
        )
    
    @classmethod
    def create_many(cls, ts_items : List[Self]) -> List[int]:
        return [ts.create_all()[0] for ts in ts_items]

    def create_all(self) -> Tuple[int, str, List[int]]:
        location_id = self.location.create()
        timeseries_id = self.create()
        values_id = TimeseriesValue.create_many(self.values)
        return (timeseries_id, location_id, values_id)

    def create(self) -> int:
        id = execStmt(
            config["user_dsn"],
            dedent("""
                INSERT INTO timeseries (location_id, parameter_id, qualifier_id, forecast_date, timestep, units) 
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (location_id, parameter_id, qualifier_id, forecast_date) 
                    DO UPDATE SET 
                        timestep=excluded.timestep, 
                        units=excluded.units 
                RETURNING id
            """),
            (self.locationId, self.parameterId, self.qualifierId, self.forecastDate, self.timestep, self.units))
        self.id = id
        return id

def downloadMgb(
        fecha_pronostico : datetime = datetime.now(),
        filterId : str = config["default_filterId"] if "default_filterId" in config else None
) -> GetTimeseriesResponse:
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

time_units = {
    "second": "seconds"
}

def parseTimestep(ts : TimeStep) -> timedelta:
    if ts["unit"] not in time_units:
        raise ValueError("Unidad de tiempo '%s' desconocida" % ts["unit"])
    ts_dict = {}
    ts_dict[time_units[ts["unit"]]] = ts["multiplier"]
    return timedelta(**ts_dict)

def parseDateTime(date : str, time : str, time_zone : float=None) -> datetime:
    tz = timezone(offset=timedelta(hours=time_zone)) if time_zone is not None else None
    return datetime.strptime(
        f"{date} {time}",
        "%Y-%m-%d %H:%M:%S",
        tzinfo=tz
    )
    
# def parseResponseItem(data : TimeseriesResponse, time_zone : float="0.0"):
#     location = Location.from_api_response(data)
#     timeseries = Timeseries.from_api_response(data, time_zone)
#     values = TimeseriesValue.from_api_response(data, time_zone)
#     return (location, timeseries, values)

if __name__ == "__main__":
    data = downloadMgb()
    json.dump(data, sys.stdout, indent=2)
    sys.stdout.write("\n")

