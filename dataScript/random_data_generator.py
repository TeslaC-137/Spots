import json
import time
import random
from datetime import datetime, timedelta
import os
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials

parkingLotsAvailable = [1320, 1295, 1265, 1245, 1250, 1300, 1320]
statusOptions = ["Occupied", "Empty"]


"""
Sample data: 
{
    "receivedAt": "2018-03-17 22:07:42",
    "parkingLot": "1365",
    "parkingSpot": 2,
    "status": "Occupied",
    "occupiedUntil": "2018-03-17 23:30:00"
}

// Change parking spot availability every 1 minute. 
"""

bucketName = "parking-lot-data-bucket"

"""

from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import os


credentials_dict = {
    'type': 'service_account',
    'client_id': os.environ['BACKUP_CLIENT_ID'],
    'client_email': os.environ['BACKUP_CLIENT_EMAIL'],
    'private_key_id': os.environ['BACKUP_PRIVATE_KEY_ID'],
    'private_key': os.environ['BACKUP_PRIVATE_KEY'],
}
credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    credentials_dict
)
client = storage.Client(credentials=credentials, project='myproject')
bucket = client.get_bucket('mybucket')
blob = bucket.blob('myfile')
blob.upload_from_filename('myfile')

"""


def create_file(filename, blobName):
    client = storage.Client.from_service_account_json("key.json")
    bucket = client.get_bucket(bucketName)
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


currTime = datetime.now()
path = "./data/"
try:
    os.mkdir(path)
except OSError:
    print("Creation of directory %s failed", path)
    exit(0)
else:
    print("Successfully created directory %s", path)

for i in range(50):
    filename = '{0}parkingLot_{1}.txt'.format(path, i)
    blobName = 'parkingLot_{0}.txt'.format(i)
    listOfJsonForABlock = []
    for j in range(20):
        listOfJsonForABlock.append(generate_parking_lot_data(currTime))
        currTime += timedelta(minutes=2)
    with open(filename, "w") as fileHandle:
        fileHandle.write("\n".join(listOfJsonForABlock))

    client = storage.Client.from_service_account_json("key.json")
    bucket = client.get_bucket(bucketName)
    blob = bucket.blob(blobName)
    print("Filename is " + filename)
    blob.upload_from_filename(filename)

    time.sleep(120)
