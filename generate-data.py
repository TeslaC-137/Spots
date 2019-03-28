#! /usr/bin/python3
import json
import time
import random
from datetime import datetime, timedelta
import os
from google.cloud import storage

parkingLotsAvailable = [1320, 1295, 1265, 1245, 1250, 1300, 1320]
statusOptions = ["Occupied", "Empty"]
STORAGE_BUCKET = os.environ.get('PARKING_BUCKET')

CLOUD_STORAGE_BUCKET = os.environ.get('CLOUD_STORAGE_BUCKET')

def create_file(filename, blobName):
    client = storage.Client()
    bucket = client.get_bucket(STORAGE_BUCKET)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)


def generate_parking_lot_data(timeReceived):
    # receivedAt = time.strftime("%Y-%m-%d %H-%M-%S", timeReceived)
    receivedAt = '{:%Y-%m-%d %H-%M-%s}'.format(timeReceived)
    parkingLot = random.choice(parkingLotsAvailable)
    parkingSpot = random.randint(1, 101)
    status = random.choice(statusOptions)
    occupiedUntil = '{:%Y-%m-%d %H-%M-%s}'.format(
        timeReceived + timedelta(minutes=2))
    return_dictionary = {
        "receivedAt": receivedAt,
        "parkingLot": parkingLot,
        "parkingSpot": parkingSpot,
        "status": status,
        "occupiedUntil": occupiedUntil
    }
    print(return_dictionary)
    return json.dumps(return_dictionary)



#path = HOME + "/parking-data"     # Create this directory in your project "mkdir -p ~/parking-data"
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
    listOfJsonForABlock = []
    for j in range(20):
        listOfJsonForABlock.append(generate_parking_lot_data(currTime))
        currTime += timedelta(minutes=2)
    with open(filename, "w") as fileHandle:
        fileHandle.write("\n".join(listOfJsonForABlock))

    client = storage.Client()
    bucket = client.get_bucket(STORAGE_BUCKET)
    blob = bucket.blob(blobName)
    print("Filename is " + filename)
    blob.upload_from_filename(filename)

    time.sleep(5)