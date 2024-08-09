import googlemaps
import json
import pandas as pd
import time
from datetime import datetime
from apikey import _MAPS_API_KEY

REGIONS = ["NORTHEAST", "MIDWEST", "SOUTH", "WEST"]
lat = "LATITUDE"
lng = "LONGITUDE"

def init_matrices():
    # write a csv file to store the distance matrix
    # if <REGION>facilities.csv has n rows, then the distance matrix will be nxn
    # the matrix will be stored in <REGION>matrix.csv
    # the matrix should have columns and rows as integers 0, ... , n-1 representing the facilities, in the same order as they were in <REGION>facilities.csv
    # also perform more cleaning on <>facilities.csv - add a new column region_index - index is currently giving the index from the original data set - where this is just a small portion of it. Let's set the index to be 0, ... , n-1 so we have a persistent ordering.
    for region in REGIONS:
        df = pd.read_csv(f"{region}facilities.csv")
        n = len(df)
        matrix = [[0 for i in range(n)] for j in range(n)]
        matrix_df = pd.DataFrame(matrix)
        matrix_df.to_csv(f"{region}matrix.csv", index=True)

def row_to_latlng(row):
    return f"{str(row[lat])},{str(row[lng])}"

def build_origins_destinations(region):
    df = pd.read_csv(f"{region}facilities.csv")
    origins = []
    for index, row in df.iterrows():
        origins.append(row_to_latlng(row))
    # origins and destinations are same
    return origins, origins

def get_response(gmaps, origins, destinations):
    # return a raw response object, and a list of distances
    time.sleep(2)
    gmatrix = gmaps.distance_matrix(origins, destinations)
    distances = []
    failed = []
    for row in gmatrix["rows"]:
        for el in row["elements"]:
            # el is a dictionary with keys:
            # distance, duration, status
            if el["status"] == "OK":
                distance_meters = el["distance"]["value"]
                distances.append(distance_meters)
            else:
                failed.append(el)
    return distances, gmatrix, failed

def get_allresponses_singleorigin(gmaps, origins, dest_splits, gmatrices):
    row = []
    total_failed = []
    for d in dest_splits:
        distances, gmatrix, failed = get_response(
            gmaps, origins, d 
        )
        row.extend(distances)
        gmatrices.append(gmatrix)
        total_failed.extend(failed)
    print(f"Origin {origins[0]} done without exceptions.")
    if len(total_failed) > 0:
        print(f"Total failed: {len(total_failed)}")
        with open (f"failed/failed_{origins[0]}.json", "w") as f:
            json.dump(total_failed, f, indent=4)
    else:
        print("No failed requests.")
    print("\n")
    return row

def main():
    # useful hardcoded values
    # origins = ["40.649782,-74.019743","41.380791,-73.517948"]
    # destinations = ["40.649782,-74.019743","41.380791,-73.517948"]
    REGION = "NORTHEAST"
    gmaps = googlemaps.Client(key=_MAPS_API_KEY)
    (origins, destinations) = build_origins_destinations(REGION)
    req1_destinations = destinations[:20]
    req2_destinations = [d for d in destinations if d not in req1_destinations]
    dest_splits = [req1_destinations, req2_destinations]
    gmatrices = []
    df_data = []
    for origin in origins:
        row = get_allresponses_singleorigin(gmaps, [origin], dest_splits, gmatrices)
        df_data.append(row)
    df = pd.DataFrame(df_data)
    df.to_csv(f"{REGION}matrix.csv", index=True)
            
    # Dump the dictionary to a JSON file just in case we need the raw response again
    with open(f"{REGION}dump.json", "w") as f:
        json.dump(gmatrices, f, indent=4)

if __name__ == "__main__":
    main()
