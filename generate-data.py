#! /usr/bin/python3
import json
import time
import random
from datetime import datetime, timedelta
import os
from google.cloud import storage

# Environmental variables
PARKING_BUCKET = os.environ.get('PARKING_BUCKET')

parkingLotsAvailable = [1350, 1295, 1265, 1245, 1250, 1300, 1365]

locations = {}      # (Latitude Logitude)
locations[1350] = (37.421396, -122.077253)
locations[1295] = (37.420275, -122.075112)
locations[1265] = (37.408717, -122.011989)
locations[1245] = (37.420351, -122.074267)
locations[1250] = (37.421364, -122.074402)
locations[1300] = (37.421350, -122.075812)
locations[1365] = (37.418487, -122.073341)

statusOptions = ["Occupied", "Empty"]


def create_file(filename, blobName):
    client = storage.Client()
    bucket = client.get_bucket(PARKING_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)


def generate_parking_lot_data(timeReceived):
    receivedAt = timeReceived.strftime('%Y-%m-%d %H:%M:%S')
    parkingLot = random.choice(parkingLotsAvailable)
    parkingSpot = random.randint(1, 101)
    status = random.choice(statusOptions)
    occupiedUntil = timeReceived + timedelta(minutes=random.randint(10, 30))
    occupiedUntil = occupiedUntil.strftime('%Y-%m-%d %H:%M:%S')
    return_dictionary = {
        "receivedAt": receivedAt,
        "parkingLot": parkingLot,
        "latitude": locations[parkingLot][0],
        "longitude": locations[parkingLot][1],
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


for i in range(50):
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

# CREATE TABLE parkingDataTable2 (
#     parkingLot INT NOT NULL,
#     parkingSpot INT NOT NULL,
#     receivedAt TIMESTAMP NOT NULL DEFAULT  '1970-01-01 00:00:01',
#     status VARCHAR(255) NOT  NULL DEFAULT 'UNKNOWN',
#     occupiedUntil TIMESTAMP NOT NULL DEFAULT  '1970-01-01 00:00:01',
#     latitude DOUBLE NOT NULL DEFAULT 0.0,
#     longitude DOUBLE NOT NULL DEFAULT 0.0,
#     CONSTRAINT PK_parkingData PRIMARY KEY (parkingLot, parkingSpot)
# );

# +---------------+--------------+------+-----+---------------------+-------+
# | Field         | Type         | Null | Key | Default             | Extra |
# +---------------+--------------+------+-----+---------------------+-------+
# | parkingLot    | int(11)      | NO   | PRI | NULL                |       |
# | parkingSpot   | int(11)      | NO   | PRI | NULL                |       |
# | receivedAt    | timestamp    | NO   |     | 1970-01-01 00:00:01 |       |
# | status        | varchar(255) | NO   |     | UNKNOWN             |       |
# | occupiedUntil | timestamp    | NO   |     | 1970-01-01 00:00:01 |       |
# | latitude      | double       | NO   |     | 0                   |       |
# | longitude     | double       | NO   |     | 0                   |       |
# +---------------+--------------+------+-----+---------------------+-------+