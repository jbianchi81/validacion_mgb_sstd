#%%
import pandas as pd
import requests
import json
#%%

sites = pd.read_csv("../static/mgb_map.csv")

locationIds = "&".join(list(set(['locationIds=%s' % str(id) for id in sites.sim])))

url = "https://sstdfews.cicplata.org/FewsWebServices/rest/fewspiservice/v1/timeseries?%s&parameterIds=Q.sim&documentFormat=PI_JSON" % locationIds

#%%
response = requests.get(url)
#%%
parsed_response = response.json()

# %%

def timeserie_to_geojson(ts):
    name = sites.loc[sites["sim"] == int(ts["header"] ["locationId"]), "name"].iat[0]
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [
                float(ts["header"]["lon"]),
                float(ts["header"]["lat"])
            ]
        },
        "properties": {
            "name": name,
            **ts["header"]
        }
    }

def timeseries_to_geojson(d):
    return {
        "type": "FeatureCollection",
        "features": [timeserie_to_geojson(ts) for ts in d["timeSeries"]]
    }
# %%

ts_geojson = timeseries_to_geojson(parsed_response)
json.dump(ts_geojson, open("../static/points.geojson", "w"), indent=2)
# %%

# %%
