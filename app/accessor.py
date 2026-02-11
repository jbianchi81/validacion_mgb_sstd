# https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries?filterId=Mod_Hydro_Output_Selected&startForecastTime=2026-01-27T00%3A00%3A00Z&endForecastTime=2026-01-28T00%3A00%3A00Z&documentFormat=PI_JSON

from datetime import datetime, timedelta, timezone, date
import requests
from typing import TypedDict, List, Self, Tuple, Optional
import json
import sys
from dataclasses import dataclass
import logging
from .utils import loadConfig, execStmt, execStmtMany, execStmtFetchAll
import psycopg
from psycopg import sql
from textwrap import dedent
import argparse

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
class Location:
    locationId : str
    stationName : str
    lat : float
    lon : float
    x : Optional[float] = None
    y : Optional[float] = None
    z : Optional[float] = None

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
            x = float(data["header"]["x"]) if "x" in data["header"] else None,
            y = float(data["header"]["y"]) if "y" in data["header"] else None,
            z = float(data["header"]["z"]) if "z" in data["header"] else None
        )


    def create(self) -> str:
        id = execStmt(
            config["user_dsn"],
            dedent("""
                INSERT INTO locations (id, station_name, geometry) 
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
class TimeseriesValue:
    time : datetime
    value : float
    flag : int
    timeseries_id : Optional[int] = None
    comment : Optional[str] = None
    id : Optional[int] = None

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
        -- RETURNING id
    """

    @classmethod
    def create_many(cls, values : List[Self], timeseries_id : int) -> int:
        """Upserts into timeseries_values

        Args:
            values (List[Self]): list of TimeseriesValues
            timeseries_id (int): timeseries identifier

        Returns:
            int: upsertion row count
        """
        rows = []
        for v in values:
            v.timeseries_id = timeseries_id
            rows.append(v.to_row())
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
class Timeseries:
    locationId : str
    parameterId : str
    timestep : timedelta
    units : str
    qualifierId : Optional[str] = None
    forecastDate : Optional[datetime] = None
    location : Optional[Location] = None
    values : Optional[List[TimeseriesValue]] = None
    id : Optional[int] = None

    @classmethod
    def from_api_response(cls, data : GetTimeseriesResponse, save : bool=False):
        time_zone = float(data["timeZone"])
        parsed = []
        for d in data["timeSeries"]:
            ts = Timeseries.parse_one(d, time_zone)
            if save:
                ts.create_all()
            parsed.append(ts)
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
            qualifierId = data["header"]["qualifierId"] if "qualifierId" in data["header"] else None,
            forecastDate = parseDateTime(data["header"]["forecastDate"]["date"], data["header"]["forecastDate"]["time"], time_zone) if "forecastDate" in data["header"] else None,
            timestep = parseTimestep(data["header"]["timeStep"]),
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
        values_count = TimeseriesValue.create_many(self.values, timeseries_id)
        return (timeseries_id, location_id, values_count)

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
            (self.locationId, self.parameterId, self.qualifierId if self.qualifierId is not None else "", self.forecastDate, self.timestep, self.units))
        self.id = id
        return id
    
    @classmethod
    def read(
        cls,
        locationId : str = None, 
        parameterId : str = None, 
        timestep : timedelta = None,
        units : str = None,
        qualifierId : str = None,
        forecastDate : datetime = None,
        id : int = None) -> List[Self]:
        
        conditions = []
        params = []

        if id is not None:
            conditions.append("id = %s")
            params.append(id)

        if locationId is not None:
            conditions.append("location_id = %s")
            params.append(locationId)

        if parameterId is not None:
            conditions.append("parameter_id = %s")
            params.append(parameterId)

        if qualifierId is not None:
            conditions.append("qualifier_id = %s")
            params.append(qualifierId)

        if forecastDate is not None:
            conditions.append("forecast_date = %s")
            params.append(forecastDate)

        if timestep is not None:
            conditions.append("timestep = %s")
            params.append(timestep)

        if units is not None:
            conditions.append("units = %s")
            params.append(units)

        sql = "SELECT * FROM timeseries"

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        ts_list = execStmtFetchAll(config["user_dsn"], sql, params)
        for ts in ts_list:
            timeseries = cls(
                locationId = ts["location_id"],
                parameterId = ts[""],
                timestep = ts["timedelta"],
                units = ts["units"],
                qualifierId = ts["qualifier_id"] if ts["qualifier_id"] != "" else None,
                forecastDate = ts["forecast_date"]
            )
        return timeseries


def download_timeseries(
        fecha_pronostico : datetime | None = None,
        filterId : str | None = None
) -> GetTimeseriesResponse:
    if fecha_pronostico is None:
        fecha_pronostico = datetime.now()
    if filterId is None:
        filterId = config["default_filterId"] if "default_filterId" in config else None
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
    ts_dict[time_units[ts["unit"]]] = int(ts["multiplier"])
    return timedelta(**ts_dict)

def parseDateTime(date : str, time : str, time_zone : float=None) -> datetime:
    dt = datetime.strptime(
        f"{date} {time}",
        "%Y-%m-%d %H:%M:%S"
    )
    if time_zone is not None:
        tz = timezone(offset=timedelta(hours=time_zone))
        dt = dt.replace(tzinfo=tz)
    return dt

    
# def parseResponseItem(data : TimeseriesResponse, time_zone : float="0.0"):
#     location = Location.from_api_response(data)
#     timeseries = Timeseries.from_api_response(data, time_zone)
#     values = TimeseriesValue.from_api_response(data, time_zone)
#     return (location, timeseries, values)

def parse_args():
    parser = argparse.ArgumentParser(description="Forecast processor")

    parser.add_argument(
        "--forecast-date",
        type=date.fromisoformat,   # expects YYYY-MM-DD
        required=False,
        help="Forecast date in YYYY-MM-DD format"
    )

    parser.add_argument(
        "--filter-id",
        type=str,
        required=False,
        help="Optional filter ID"
    )

    parser.add_argument(
        "--output",
        type=str,
        required=False,
        help="Output file"
    )

    parser.add_argument(
        "--save",
        action="store_true",
        help="Save into database"
    )

    parser.add_argument(
        "--input",
        type=str,
        required=False,
        help="Input file. If not set, downloads from API source using --forecast-date and --filter-id"
    )

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.input is not None:
        with open(args.input, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = download_timeseries(args.forecast_date, args.filter_id)
        if args.output is not None:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
    # else:
    #     json.dump(data, sys.stdout, indent=2)
    #     sys.stdout.write("\n")
    if args.save:
        Timeseries.from_api_response(data, True)
        
