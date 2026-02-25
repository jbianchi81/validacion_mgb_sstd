# Validación de pronósticos SSTD-FEWS
## Objetivo
Almacenar en base de datos las observaciones y los pronósticos del SSTD-FEWS para calcular indicadores de eficiencia de los pronósticos. Descarga series temporales de la API REST de SSTD-FEWS (https://sstdfews.cicplata.org/FewsWebServices/test/fewspiservicerest/apidoc/apidoc.html)
## Requerimientos:
- python 3
- postgresql
## Instalación y configuración
```bash
# genera virtualenv
python -m venv .venv
# activa virtualenv
source .venv/bin/activate
# instala dependencias de python
pip install -r requirements.txt
# luego editar el archivo de configuración ingresando los parámetros de conexión a la base de datos
nano config/config.json
# generar base de datos
python -m app.createdb
``` 
## Uso
### Accessor
```
python -m app.accessor --help
usage: accessor.py [-h] [--forecast-date FORECAST_DATE] [--filter-id FILTER_ID] [--output OUTPUT] [--file-pattern FILE_PATTERN] [--save]
                   [--input INPUT] [--location-id [LOCATION_ID ...]] [--parameter-id [PARAMETER_ID ...]]
                   [--qualifier-id [QUALIFIER_ID ...]] [--timestart TIMESTART] [--timeend TIMEEND] [--format {json,csv}]
                   {get,read,delete}

Forecast processor

positional arguments:
  {get,read,delete}     Action to perform

options:
  -h, --help            show this help message and exit
  --forecast-date FORECAST_DATE
                        Forecast date in YYYY-MM-DD format. If not set, with 'get' last forecast is retrieved, with 'read'/'delete' all
                        forecasts are read/deleted.
  --filter-id FILTER_ID
                        Optional filter ID
  --output OUTPUT       Output file
  --file-pattern FILE_PATTERN
                        Output file pattern. May use T for forecast date, L for location id, P for parameter id and I for timeseries id
  --save                Save into database
  --input INPUT         Input file. If not set, downloads from API source using --forecast-date and --filter-id
  --location-id [LOCATION_ID ...]
                        read only timeseries of this location(s)
  --parameter-id [PARAMETER_ID ...]
                        read only timeseries of this parameter(s)
  --qualifier-id [QUALIFIER_ID ...]
                        read only timeseries of this qualifier(s)
  --timestart TIMESTART
                        read only values starting from this date. If no timestart and timeend are specified, with 'get' the requested
                        period will be set to the current time minus one day and one hour ago until the current time plus one day and
                        one hour. If only the timestsart is specified, the requested period will be set to the timestart until the
                        timestart time plus one day and one hour. If only the timeend is specified, the requested period will be set to
                        the timeend minus one day and one hour until the timeend.With 'read'/'delete' all dates with be read/deleted
  --timeend TIMEEND     read only values before this date. If no timestart and timeend are specified, with 'get' the requested period
                        will be set to the current time minus one day and one hour ago until the current time plus one day and one hour.
                        If only the timestsart is specified, the requested period will be set to the timestart until the timestart time
                        plus one day and one hour. If only the timeend is specified, the requested period will be set to the timeend
                        minus one day and one hour until the timeend.With 'read'/'delete' all dates with be read/deleted
  --format {json,csv}   Output format: json, csv. Default: json
```
#### Ejemplos
Descargar corrida del MGB de la fecha 2026-02-24 para las estaciones seleccionadas en el filtro por defecto (Mod_Hydro_Output_Selected). Guardar en la base de datos y en data/mgb.json 
```bash
python -m app.accessor get --forecast-date 2026-02-24 --output data/mgb.json --save
```
Descargar última corrida del MGB para la estación 1002 del filtro Mod_Hydro_Output_All entre las fechas 2026-02-24 y 2026-03-02. Guardar en data/mgb_1002.json 
```bash
python -m app.accessor get --filter-id Mod_Hydro_Output_All --location-id 1002 --timestart 2026-02-24 --timeend 2026-03-02 --output data/mgb_1002.json
```
Descargar serie de caudal observado (Q.obs) de la estación Corrientes (ID=AR_INA_19_INA_24_Q) entre las fechas 2025-02-01 y 2026-02-25. Guarda en base de datos y en data/corr.json
```bash
python -m app.accessor get --filter-id Tablero_Hydro --location-id AR_INA_19_INA_24_Q --parameter-id Q.obs --timestart 2025-02-01 --timeend 2026-02-25 --output data/corr.json --save
```
Leer serie guardada en base de datos y escribir en archivo CSV
```bash
python -m app.accessor read --location-id AR_INA_19_INA_24_Q --parameter-id Q.obs --timestart 2025-02-01 --timeend 2026-02-25 --output data/corr.csv --format csv
```
## Créditos
Instituto Nacional del Agua - Argentina - 2026
