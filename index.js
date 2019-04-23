// Create Cloud SQL
// gcloud sql instances create parking-sql-instance
// gcloud sql instances describe parking-sql-instance |grep connectionName
// connectionName: intro2cloudcomputing18:us-west1:parking-sql-instance

// gcloud sql connect parking-sql-instance --user=root
// CREATE DATABASE parkingDatabase;
// USE parkingDatabase

// CREATE TABLE parkingDataTable3 (
//     parkingLot VARCHAR(255) NOT NULL,
//     parkingSpot INT NOT NULL,
//     receivedAt TIMESTAMP NOT NULL DEFAULT  '1970-01-01 00:00:01',
//     status VARCHAR(255) NOT  NULL DEFAULT 'UNKNOWN',
//     occupiedUntil TIMESTAMP NOT NULL DEFAULT  '1970-01-01 00:00:01',
//     latitude DOUBLE NOT NULL DEFAULT 0.0,
//     longitude DOUBLE NOT NULL DEFAULT 0.0,
//     address VARCHAR(255) NOT NULL,
//     CONSTRAINT PK_parkingData PRIMARY KEY (parkingLot, parkingSpot)
// );

// +---------------+--------------+------+-----+---------------------+-------+
// | Field         | Type         | Null | Key | Default             | Extra |
// +---------------+--------------+------+-----+---------------------+-------+
// | parkingLot    | varchar(255) | NO   | PRI | NULL                |       |
// | parkingSpot   | int(11)      | NO   | PRI | NULL                |       |
// | receivedAt    | timestamp    | NO   |     | 1970-01-01 00:00:01 |       |
// | status        | varchar(255) | NO   |     | UNKNOWN             |       |
// | occupiedUntil | timestamp    | NO   |     | 1970-01-01 00:00:01 |       |
// | latitude      | double       | NO   |     | 0                   |       |
// | longitude     | double       | NO   |     | 0                   |       |
// | address       | varchar(255) | NO   |     | NULL                |       |
// +---------------+--------------+------+-----+---------------------+-------+


// const process = require('process'); // Allow env variable mocking
const mysql = require('mysql');

const connectionName = 'intro2cloudcomputing18:us-west1:parking-sql-instance';
const dbUser = 'root';
const dbPassword = 'password123';
const dbName = 'parkingDatabase';
const parkingTable = 'parkingDataTable3';

const mysqlConfig = {
    connectionLimit: 1,
    user: dbUser,
    password: dbPassword,
    database: dbName,
    socketPath: `/cloudsql/${connectionName}`
};

// Connection pools reuse connections between invocations,
// and handle dropped or expired connections automatically.
let mysqlPool;

/**
 * Background Cloud Function to be triggered by PubSub.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
exports.pubSubToCloudSQL = (event, callback) => {
    const PubSubMessage = event.data;

    // Incoming data is in JSON format
    const incomingData = PubSubMessage.data ? Buffer.from(PubSubMessage.data, 'base64').toString()
        : "{'receivedAt':'1970-01-01 00:00:01','parkingLot':'UNKNOWN','parkingSpot':0,'status':'UNKNOWN','occupiedUntil':'1970-01-01 00:00:01','address':'N/A','latitude':0,'longitude':0}";
    const row = JSON.parse(incomingData);

    // Print incoming and update data.
    console.log(`Incoming data: ${incomingData}`);


    // Initialize the pool lazily, in case SQL access isn't needed for this
    // GCF instance. Doing so minimizes the number of active SQL connections,
    // which helps keep your GCF instances under SQL connection limits.
    if (!mysqlPool) {
        mysqlPool = mysql.createPool(mysqlConfig);
    }

    const queryString = "INSERT INTO " + parkingTable +
          				" (parkingLot, parkingSpot, receivedAt, status, occupiedUntil, address, latitude, longitude) " +
                        "VALUES ('"+
                            row.parkingLot + "', " +
                            row.parkingSpot.toString() + ", '" +
                            row.receivedAt + "', '" +
                            row.status + "', '" +
                            row.occupiedUntil + "', '" +
          					row.address + "', " +
          					row.latitude.toString() + ", " +
          					row.longitude.toString() +
                        ") " +
                        "ON DUPLICATE KEY UPDATE " +
                            "receivedAt=VALUES(receivedAt), " +
                            "status=VALUES(status)," +
                            "occupiedUntil=VALUES(occupiedUntil)," +
          					"address=VALUES(address)," +
          					"latitude=VALUES(latitude)," +
          					"longitude=VALUES(longitude)";

    mysqlPool.query(queryString,
        (err, results) => {
        if (err) {
            console.error(err);
        }
    });

    // Close any SQL resources that were declared inside this function.
    // Keep any declared in global scope (e.g. mysqlPool) for later reuse.
    callback();
};