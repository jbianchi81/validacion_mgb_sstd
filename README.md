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
usage: accessor.py [-h] [--forecast-date FORECAST_DATE] [--filter-id FILTER_ID] [--output OUTPUT]
                   [--file-pattern FILE_PATTERN] [--save] [--input INPUT]
                   [--location-id LOCATION_ID] [--parameter-id PARAMETER_ID] [--timestart TIMESTART]
                   [--timeend TIMEEND] [--format {json,csv}]
                   {get,read,delete}

Forecast processor

positional arguments:
  {get,read,delete}  Action to perform

options:
  -h, --help            show this help message and exit
  --forecast-date FORECAST_DATE
                        Forecast date in YYYY-MM-DD format
  --filter-id FILTER_ID
                        Optional filter ID
  --output OUTPUT       Output file
  --file-pattern FILE_PATTERN
                        Output file pattern. May use T for forecast date, L for location id, P for
                        parameter id and I for timeseries id
  --save                Save into database
  --input INPUT         Input file. If not set, downloads from API source using --forecast-date and
                        --filter-id
  --location-id LOCATION_ID
                        read only timeseries of this location
  --parameter-id PARAMETER_ID
                        read only timeseries of this parameter
  --timestart TIMESTART
                        read only values starting from this date
  --timeend TIMEEND     read only values before this date
  --format {json,csv}   Output format: json, csv. Default: json
```
#### Ejemplos
Descargar última corrida del MGB para las estaciones seleccionadas en el filtro por defecto (Mod_Hydro_Output_Selected). Guardar en la base de datos y en data/mgb.json 
```bash
python -m app.accessor get --output data/mgb.json --save
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
