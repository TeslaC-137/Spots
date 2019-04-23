#! /usr/bin/python3
import json
import time
import random
from datetime import datetime, timedelta
import os
from google.cloud import storage

# Environmental variables
PARKING_BUCKET = os.environ.get('PARKING_BUCKET')

parkingLotsAvailable = ["Google Parking Lot 1365", "Google Parking Lot 1295",
                        "Google Parking Lot 1950", "Google Parking Lot 1212",
                        "Google Parking Lot MAT1"]

locations = {}      # (Latitude Logitude)
locations["Google Parking Lot 1365"] = ("Google Building 1365, Charleston Road, Mountain View, CA",
                                            (37.418428, -122.073320))
locations["Google Parking Lot 1295"] = ("Google Building 1295, Charleston Road, Mountain View, CA",
                                            (37.420177, -122.075123))
locations["Google Parking Lot 1950"] = ("Google Building 1950, Charleston Road, Mountain View, CA",
                                            (37.422125, -122.087583))
locations["Google Parking Lot 1212"] = ("Google Building 1212, Bordeaux Drive, Sunnyvale, CA",
                                            (37.410230, -122.022211))
locations["Google Parking Lot MAT1"] = ("Google Building MAT1, North Mathilda Avenue, Sunnyvale, CA",
                                            (37.408660, -122.026474))

statusOptions = ["Occupied", "Empty"]

def create_file(filename, blobName):
    client = storage.Client()
    bucket = client.get_bucket(PARKING_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)


def generate_parking_lot_data(timeReceived):
    receivedAt = timeReceived.strftime('%Y-%m-%d %H:%M:%S')
    parkingLot = random.choice(parkingLotsAvailable)
    parkingSpot = random.randint(1, 11)
    status = random.choice(statusOptions)
    occupiedUntil = timeReceived + timedelta(minutes=random.randint(10, 30))
    occupiedUntil = occupiedUntil.strftime('%Y-%m-%d %H:%M:%S')
    return_dictionary = {
        "receivedAt": receivedAt,
        "parkingLot": parkingLot,
        "address": locations[parkingLot][0],
        "latitude": locations[parkingLot][1][0],
        "longitude": locations[parkingLot][1][1],
        "parkingSpot": parkingSpot,
        "status": status,
        "occupiedUntil": occupiedUntil
    }
    print(return_dictionary)
    return json.dumps(return_dictionary) + "\n"


path = './data'
try:
    if not os.path.exists(path):
        os.mkdir(path)
except OSError:
    print("Creation of directory %s failed", path)
    exit(1)
else:
    print("Successfully created directory %s", path)


for i in range(5000):
    currTime = datetime.now()
    filename = '{0}/parkingLot_{1}.json'.format(path, i)
    blobName = 'parkingLot_{0}.json'.format(i)
    listOfJsonForABlock = ""
    for j in range(20):
        listOfJsonForABlock += generate_parking_lot_data(currTime)
        currTime += timedelta(minutes=2)
    with open(filename, "w") as fileHandle:
        fileHandle.write(listOfJsonForABlock)

    client = storage.Client()
    bucket = client.get_bucket(PARKING_BUCKET)
    blob = bucket.blob(blobName)
    print("Filename is " + filename)
    blob.upload_from_filename(filename)

    time.sleep(5)

# CREATE TABLE parkingDataTable3 (
#     parkingLot VARCHAR(255) NOT NULL,
#     parkingSpot INT NOT NULL,
#     receivedAt TIMESTAMP NOT NULL DEFAULT  '1970-01-01 00:00:01',
#     status VARCHAR(255) NOT  NULL DEFAULT 'UNKNOWN',
#     occupiedUntil TIMESTAMP NOT NULL DEFAULT  '1970-01-01 00:00:01',
#     latitude DOUBLE NOT NULL DEFAULT 0.0,
#     longitude DOUBLE NOT NULL DEFAULT 0.0,
#     address VARCHAR(255) NOT NULL,
#     CONSTRAINT PK_parkingData PRIMARY KEY (parkingLot, parkingSpot)
# );

# +---------------+--------------+------+-----+---------------------+-------+
# | Field         | Type         | Null | Key | Default             | Extra |
# +---------------+--------------+------+-----+---------------------+-------+
# | parkingLot    | varchar(255) | NO   | PRI | NULL                |       |
# | parkingSpot   | int(11)      | NO   | PRI | NULL                |       |
# | receivedAt    | timestamp    | NO   |     | 1970-01-01 00:00:01 |       |
# | status        | varchar(255) | NO   |     | UNKNOWN             |       |
# | occupiedUntil | timestamp    | NO   |     | 1970-01-01 00:00:01 |       |
# | latitude      | double       | NO   |     | 0                   |       |
# | longitude     | double       | NO   |     | 0                   |       |
# | address       | varchar(255) | NO   |     | NULL                |       |
# +---------------+--------------+------+-----+---------------------+-------+