// =============================================================================
// mongodb/init/00_replica_set.js
// Initialises the MongoDB replica set (required by Debezium CDC).
// Debezium reads change events from the MongoDB oplog, which is only
// available when the instance runs as a replica set member.
// Even a single-node deployment must be initialised with rs.initiate().
// =============================================================================

// rs.initiate() only succeeds on the primary, and only once.
// Running it again returns a harmless "already initialized" error.
try {
    const result = rs.initiate({
        _id: 'rs0',
        members: [{ _id: 0, host: 'mongodb:27017' }]
    });
    print('[initdb] Replica set rs0 initiated: ' + JSON.stringify(result));
} catch (e) {
    if (e.codeName === 'AlreadyInitialized') {
        print('[initdb] Replica set rs0 already initialised — skipping');
    } else {
        throw e;
    }
}

// Wait for the primary to be elected before returning
let attempts = 0;
while (rs.status().myState !== 1 && attempts < 20) {
    sleep(500);
    attempts++;
}
print('[initdb] Replica set primary ready. State: ' + rs.status().myState);
