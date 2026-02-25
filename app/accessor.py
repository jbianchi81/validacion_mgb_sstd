from __future__ import annotations
from datetime import datetime, timedelta, timezone, date
import requests
from typing import TypedDict, List, Tuple, Optional, Union, Iterator
from typing_extensions import Self
import json
from dataclasses import dataclass, asdict
import logging
from .utils import loadConfig, execStmt, execStmtMany, execStmtFetchAll
from textwrap import dedent
import argparse
import pandas as pd
import sys
# from collections.abc import Iterator
from urllib.parse import urlencode

# startForecastTime = "2026-01-27T00%3A00%3A00Z"
# endForecastTime = "2026-01-28T00%3A00%3A00Z"
documentFormat = "PI_JSON"
config_path = "config/config.json"

SENTINEL = datetime(1900, 1, 1, tzinfo=timezone.utc)

config = loadConfig(config_path)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

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

class TimeseriesKey(TypedDict):
    locationId : str
    parameterId : str
    qualifierId : Optional[str]
    forecastDate : Optional[datetime]

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
    
    @classmethod
    def read_one(cls, locationId : str):
        matches = execStmtFetchAll(
            config["user_dsn"], 
            """SELECT id, station_name, st_x(geometry) lon, st_y(geometry) lat FROM locations WHERE id=%s""",
            (locationId,)
        )
        if not len(matches):
            raise ValueError("No se encontró la location con id=%s" % locationId)
        return cls(
            locationId = matches[0]["id"],
            stationName = matches[0]["station_name"],
            lat = matches[0]["lat"],
            lon = matches[0]["lon"]
        )

@dataclass
class TimeseriesValue:
    time : datetime
    value : float
    flag : int
    timeseries_id : Optional[int] = None
    comment : Optional[str] = None
    id : Optional[int] = None

    @classmethod
    def parse_one(cls, event : Event, time_zone : float=0.0, null_value : Optional[float] = None):
    # def parseValue(event : Event, time_zone : float=0.0) -> TimeseriesValue:
        value = float(event["value"])
        value = None if null_value is not None and value == null_value else value
        return cls(
            time = parseDateTime(event["date"], event["time"], time_zone),
            value = value,
            flag = int(event["flag"])
        )

    @classmethod
    def from_api_response(cls, data : TimeseriesResponse, time_zone : float=0.0, null_value : Optional[float]=None):
    # def parseValues(data : TimeseriesResponse, time_zone : float=0.0) -> List[TimeseriesValue]:
        if "events" not in data:
            raise ValueError("No se encontraron timeseries. Falta 'events' en la respuesta de /timeseries.")
        return [ cls.parse_one(event, time_zone, null_value) for event in data["events"] ]

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
            if v.value is None:
                continue
            v.timeseries_id = timeseries_id
            rows.append(v.to_row())
        return execStmtMany(
            config["user_dsn"],
            cls.create_stmt,
            rows
        )
        
    def create(self) -> str:
        if self.value is None:
            raise ValueError("Can't create: value is None")
        id = execStmt(
            config["user_dsn"],
            dedent(self.create_stmt),
            self.to_row())
        self.id = id
        return id
    
    @classmethod
    def read(
        cls, 
        timeseries_id : int = None, 
        time : datetime = None,
        timestart : datetime = None, 
        timeend : datetime = None,
        id : int = None,
        value : float = None,
        flag : str = None,
        comment : str = None) -> List[Self]:
        conditions = []
        params = []
        if timeseries_id is not None:
            conditions.append("series_id = %s")
            params.append(timeseries_id)

        if time is not None:
            conditions.append("time = %s")
            params.append(time)

        if timestart is not None:
            conditions.append("time >= %s")
            params.append(timestart)

        if timeend is not None:
            conditions.append("time <= %s")
            params.append(timeend)

        if id is not None:
            conditions.append("id = %s")
            params.append(id)

        if value is not None:
            conditions.append("value = %s")
            params.append(value)

        if flag is not None:
            conditions.append("flag = %s")
            params.append(flag)

        if comment is not None:
            conditions.append("comment = %s")
            params.append(comment)

        sql = "SELECT * FROM timeseries_values"

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        sql += " ORDER BY series_id, time"

        matches = execStmtFetchAll(config["user_dsn"], sql, params)
        ts_values = []
        for match in matches:
            ts_value = cls(
                timeseries_id = match["series_id"], 
                time = match["time"],
                value = match["value"],
                flag = match["flag"],
                comment = match["comment"],
                id = match["id"]
            )
            ts_values.append(ts_value)
        return ts_values




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
            values = TimeseriesValue.from_api_response(data, time_zone, float(data["header"]["missVal"]) if "missVal" in data["header"] else None)
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
            (self.locationId, self.parameterId, self.qualifierId if self.qualifierId is not None else "", self.forecastDate or SENTINEL, self.timestep, self.units))
        self.id = id
        return id
    
    @classmethod
    def read(
        cls,
        locationId : Union[str,List[str],None] = None, 
        parameterId : Union[str,List[str],None] = None, 
        timestep : Optional[timedelta] = None,
        units : Optional[str] = None,
        qualifierId : Union[str,List[str],None] = None,
        forecastDate : Optional[datetime] = None,
        id : Optional[int] = None,
        timestart : Optional[datetime] = None,
        timeend : Optional[datetime] = None,
        metadata_only : bool = False) -> Iterator[Self]:
        
        conditions = []
        params = []

        if id is not None:
            conditions.append("id = %s")
            params.append(id)

        if locationId is not None:
            conditions.append("location_id = ANY(%s)")
            params.append([locationId] if type(locationId) == str else locationId)

        if parameterId is not None:
            conditions.append("parameter_id = ANY(%s)")
            params.append([parameterId] if type(parameterId) == str else parameterId)

        if qualifierId is not None:
            conditions.append("qualifier_id = ANY(%s)")
            params.append([qualifierId]  if type(qualifierId) == str else qualifierId)

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
                parameterId = ts["parameter_id"],
                timestep = ts["timestep"],
                units = ts["units"],
                qualifierId = ts["qualifier_id"] if ts["qualifier_id"] != "" else None,
                forecastDate = ts["forecast_date"] if ts["forecast_date"] != SENTINEL else None,
                id = ts["id"]
            )
            timeseries.read_location()
            if not metadata_only:
                timeseries.read_values(timestart, timeend)
            yield timeseries

    def read_location(self):
        self.location = Location.read_one(self.locationId)

    def read_values(self, timestart : datetime = None, timeend : datetime = None):
        if self.id is None:
            raise ValueError("Falta id de timeseries, no se pueden leer los valores")
        self.values = TimeseriesValue.read(timeseries_id=self.id, timestart=timestart, timeend=timeend)

    def to_dict(self, json_serializable : bool=False, include_id : bool = True):
        data = asdict(self)
        if json_serializable:
            data["forecastDate"] = data["forecastDate"].isoformat() if data["forecastDate"] is not None else None
            data["timestep"] = {"seconds": int(data["timestep"].total_seconds())}
            for value in data["values"]:
                if not include_id:
                    del value["id"]
                    del value["timeseries_id"]
                value["time"] = value["time"].isoformat()
        return data
    
    def to_df(self) -> pd.DataFrame:
        d = self.to_dict()
        return pd.DataFrame(d["values"])
    
    def to_json(self, filename : str):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({"timeSeries": [self.to_dict(True)]}, f, indent=2)
        
    def to_csv(self, filename : str, include_id : bool = False):
        with open(filename, "w", encoding="utf-8") as f:
            df = self.to_df()
            if not include_id:
                df = df.drop(columns=["id"])
            df.to_csv(f, index=False)

    def to_file(self, filename : str, include_id : bool = False, format : str = "json"):
        if format == "json":
            self.to_json(filename)
        elif format == "csv":
            self.to_csv(filename, include_id)
        else:
            raise ValueError("Invalid format: %s" % (format))
        logging.info("Se escribió el archivo %s" % (filename))
    
    @classmethod
    def to_df_many(cls, ts_list : List[Self]) -> pd.DataFrame:
        values = []
        for ts in ts_list:
            ts_d = ts.to_dict()
            values.extend(ts_d["values"])
        return pd.DataFrame(values)
    
    def filename_from_pattern(self, file_pattern, check_placeholders : bool = False) -> str:
        if check_placeholders:
            if "{L}" not in file_pattern:
                raise ValueError("Falta '{L}' (location id) en el patrón de archivo")
            if "{P}" not in file_pattern:
                raise ValueError("Falta '{P}' (parameter id) en el patrón de archivo")
            if self.forecastDate and "{T}" not in file_pattern:
                raise ValueError("Falta '{T}' (forecast date) en el patrón de archivo")    
        filename = file_pattern.replace("{L}",self.locationId).replace("{P}",self.parameterId)
        if self.forecastDate is not None:
            filename = filename.replace("{T}",self.forecastDate.isoformat()[0:10]) 
        if self.id is not None:
            filename = filename.replace("{I}",str(self.id)) 

        return filename
        
    @classmethod
    def to_file_many(
        cls, 
        ts_list : List[Self], 
        filename : Optional[str] = None, 
        file_pattern : Optional[str] = None, 
        format : str = "json", 
        include_id : bool = False):
        if filename is not None:
            with open(filename, "w", encoding="utf-8") as f:
                if format == "csv":
                    df = cls.to_df_many(ts_list)
                    if not include_id and "id" in df.columns:
                        df = df.drop(columns=["id"])    
                    df.to_csv(f, index=False)
                elif format == "json":
                    json.dump({"timeSeries":[ts.to_dict(True, include_id=include_id) for ts in ts_list]}, f, indent=2)
                else:
                    raise ValueError("Unknown format: %s" % format)
                logging.info("Se guardó el archivo %s" % (args.output))
        elif file_pattern is not None:
            for ts in ts_list:
                fname = ts.filename_from_pattern(file_pattern)
                if format == "csv":
                    df = ts.to_df()
                    if not include_id:
                        df = df.drop(columns=["id"])
                    df.to_csv(fname, index=False)
                elif format == "json":
                    with open(fname, "w", encoding="utf-8") as f:
                        json.dump({"timeSeries":[ts.to_dict(True, include_id=include_id)]}, f, indent=2)
                logging.info("Se escribió el archivo %s" % (fname))
        else:
            raise ValueError("Falta filename o file_pattern")

    @classmethod
    def read_one(
        cls,
        locationId : str,
        parameterId : str,
        qualifierId : str = "",
        forecastDate : Optional[datetime] = None,
        timestart : Optional[datetime] = None,
        timeend : Optional[datetime] = None,
        metadata_only : bool = False
    ) -> Self:
        ts = next(cls.read(
            locationId=locationId,
            parameterId=parameterId,
            qualifierId=qualifierId,
            forecastDate=forecastDate,
            timestart=timestart,
            timeend=timeend,
            metadata_only=metadata_only
        ), None)
        if ts is None:
            raise ValueError("Timeseries not found")
        return ts

    @classmethod
    def read_paired(
        cls,
        obs_key : TimeseriesKey,
        sim_key : TimeseriesKey,
        timestart : Optional[datetime] = None,
        timeend : Optional[datetime] = None,
        obs_flag : Optional[str] = None,
        sim_flag : Optional[str] = None
    ) -> pd.DataFrame:
        obs = cls.read_one(**obs_key, metadata_only=True)
        sim = cls.read_one(**sim_key, metadata_only=True)
        return read_paired(obs.id, sim.id, timestart, timeend, obs_flag, sim_flag)

    @classmethod
    def readlist(
        cls,
        **kwargs
    ) -> List[Self]:
        ts_list = []
        for ts in Timeseries.read(
            **kwargs
        ):
            ts_list.append(ts)
        return ts_list

    @classmethod
    def read_to_file(      
        cls,
        filename : Optional[str] = None, 
        file_pattern : Optional[str] = None, 
        format : str = "json", 
        include_id : bool = False,
        **kwargs
        # forecastDate :  = args.forecast_date,
        # locationId = args.location_id,
        # parameterId = args.parameter_id,
        # timestart = args.timestart, 
        # timeend = args.timeend,
        # qualifierId = args.qualifier_id
    ):
        if filename is not None:
            ts_list = cls.readlist(**kwargs)
            logging.info("Se leyeron %i series temporales" % (len(ts_list)))
            cls.to_file_many(ts_list, filename, format = format, include_id=include_id)
        elif file_pattern is not None:
            for ts in Timeseries.read(
                **kwargs
                # forecastDate = args.forecast_date,
                # locationId = args.location_id,
                # parameterId = args.parameter_id,
                # timestart = args.timestart, 
                # timeend = args.timeend,
                # qualifierId = args.qualifier_id
            ):
                fname = ts.filename_from_pattern(file_pattern)
                ts.to_file(fname, include_id, format=format)
        else:
            raise ValueError("Falta filename o file_pattern")  

def read_paired(
    obs_series_id : int, 
    sim_series_id : int, 
    timestart : Optional[datetime] = None, 
    timeend : Optional[datetime] = None, 
    obs_flag : Optional[int] = None, 
    sim_flag : Optional[int] = None
    ) -> pd.DataFrame:
    sql = """
        SELECT
            o.time,
            o.value AS obs,
            s.value AS sim
        FROM timeseries_values o
        JOIN timeseries_values s
            ON o.time = s.time
        WHERE o.series_id = %s
        AND s.series_id = %s
        """
    params = [obs_series_id, sim_series_id]
    conditions = []
    if timestart is not None:
        conditions.append("o.time >= %s")
        params.append(timestart)
    if timeend is not None:
        conditions.append("o.time < %s")
        params.append(timeend)
    if obs_flag is not None:
        conditions.append("o.flag = %s")
        params.append(obs_flag)
    if sim_flag is not None:
        conditions.append("s.flag = %s")
        params.append(sim_flag)
    if len(conditions):
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY o.time"

    data = execStmtFetchAll(config["user_dsn"], sql, params)
    return pd.DataFrame(data)

def download_timeseries(
        fecha_pronostico : Optional[datetime] = None,
        filterId : Optional[str] = None,
        locationIds : Union[str,List[str],None] = None,
        parameterIds : Union[str,List[str],None] = None,
        timestart : Optional[datetime] = None,
        timeend : Optional[datetime] = None,
        qualifierIds : Union[str,List[str],None] = None
) -> GetTimeseriesResponse:
    # https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries?filterId=Mod_Hydro_Output_Selected&startForecastTime=2026-01-27T00%3A00%3A00Z&endForecastTime=2026-01-28T00%3A00%3A00Z&documentFormat=PI_JSON

    # if fecha_pronostico is None:
    #     fecha_pronostico = datetime.now()
    if filterId is None:
        filterId = config["default_filterId"] if "default_filterId" in config else None
    inicio = datetime(fecha_pronostico.year, fecha_pronostico.month, fecha_pronostico.day) if fecha_pronostico is not None else None
    fin = inicio + timedelta(days=1) if inicio is not None else None
    startForecastTime = "%sZ" % (inicio.isoformat(timespec='seconds')) if inicio is not None else None
    endForecastTime =  "%sZ" % (fin.isoformat(timespec='seconds')) if fin is not None else None
    startTime = "%sZ" % (timestart.isoformat(timespec='seconds')) if timestart is not None else None
    endTime = "%sZ" % (timeend.isoformat(timespec='seconds')) if timeend is not None else None
    url = "%s/timeseries" % (config["base_url"])
    params = {
            "filterId": filterId, 
            "startForecastTime": startForecastTime, 
            "endForecastTime": endForecastTime, 
            "locationIds": locationIds,
            "parameterIds": parameterIds,
            "documentFormat": documentFormat,
            "startTime": startTime,
            "endTime": endTime,
            "qualifierIds": qualifierIds
        }
    # logging.debug(f'GET {url}?{urlencode(params)}')
    response = requests.get(
        url, 
        params
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
        f'{date} {time}',
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

ACTIONS = ["get", "read", "delete"]

def parse_args():
    parser = argparse.ArgumentParser(description="Forecast processor")

    parser.add_argument(
        "action",
        choices=ACTIONS,
        help="Action to perform"
    )

    parser.add_argument(
        "--forecast-date",
        type=date.fromisoformat,   # expects YYYY-MM-DD
        required=False,
        help="Forecast date in YYYY-MM-DD format. If not set, with 'get' last forecast is retrieved, with 'read'/'delete' all forecasts are read/deleted."
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
        "--file-pattern",
        type=str,
        required=False,
        help="Output file pattern. May use T for forecast date, L for location id, P for parameter id and I for timeseries id"
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

    parser.add_argument(
        "--location-id",
        type=str,
        required=False,
        help="read only timeseries of this location(s)",
        nargs="*"
    )

    parser.add_argument(
        "--parameter-id",
        type=str,
        required=False,
        help="read only timeseries of this parameter(s)",
        nargs="*"
    )

    parser.add_argument(
        "--qualifier-id",
        type=str,
        required=False,
        help="read only timeseries of this qualifier(s)",
        nargs="*"
    )

    parser.add_argument(
        "--timestart",
        type=date.fromisoformat,   # expects YYYY-MM-DD
        required=False,
        help="read only values starting from this date. If no timestart and timeend are specified, with 'get' the requested period will be set to the current time minus one day and one hour ago until the current time plus one day and one hour. If only the timestsart is specified, the requested period will be set to the timestart until the timestart time plus one day and one hour. If only the timeend is specified, the requested period will be set to the timeend minus one day and one hour until the timeend.With 'read'/'delete' all dates with be read/deleted"
    )

    parser.add_argument(
        "--timeend",
        type=date.fromisoformat,   # expects YYYY-MM-DD
        required=False,
        help="read only values before this date. If no timestart and timeend are specified, with 'get' the requested period will be set to the current time minus one day and one hour ago until the current time plus one day and one hour. If only the timestsart is specified, the requested period will be set to the timestart until the timestart time plus one day and one hour. If only the timeend is specified, the requested period will be set to the timeend minus one day and one hour until the timeend.With 'read'/'delete' all dates with be read/deleted"
    )

    parser.add_argument(
        "--format",
        choices=["json","csv"],
        required=False,
        default="json",
        help="Output format: json, csv. Default: json"
    )

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    timestart = datetime.combine(args.timestart, datetime.min.time()) if args.timestart is not None else None
    timeend = datetime.combine(args.timeend, datetime.min.time()) if args.timeend is not None else None


    if args.action == "get":
        if args.input is not None:
            with open(args.input, "r", encoding="utf-8") as f:
                data = json.load(f)
            Timeseries.from_api_response(data, True)
        else:
            if args.output is None and not args.save:
                raise ValueError("Debe utilizar la opción --output y/o --save")
            data = download_timeseries(args.forecast_date, args.filter_id, args.location_id, args.parameter_id, timestart, timeend, args.qualifier_id)
            if args.output is not None:
                with open(args.output, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
            # else:
            #     json.dump(data, sys.stdout, indent=2)
            #     sys.stdout.write("\n")
            if args.save:
                Timeseries.from_api_response(data, True)

    elif args.action == "read":
        Timeseries.read_to_file(
            filename = args.output, 
            file_pattern = args.file_pattern, 
            format=args.format,
            forecastDate = args.forecast_date,
            locationId = args.location_id,
            parameterId = args.parameter_id,
            timestart = timestart, 
            timeend = timeend,
            qualifierId = args.qualifier_id
        )

    elif args.action == "delete":
        logging.warning("No implementado")

    else:
        raise ValueError("Argumento 'action' incorrecto. Valores válidos: %s" % (", ".join(ACTIONS)))
        
        
