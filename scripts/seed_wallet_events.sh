#!/bin/bash

# Configuration
CONTAINER_NAME="ebc-mongodb"
DATABASE_NAME="meeza_digital"
COLLECTION_NAME="wallet_events"

echo "Checking if $CONTAINER_NAME is running..."
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "Container is active. Starting data insertion..."
    
    # Run mongosh inside the container using a heredoc
    docker exec -i $CONTAINER_NAME mongosh --quiet <<EOF
use $DATABASE_NAME;

// Define helper data
const eventTypes = ['TOP_UP', 'QR_PAYMENT', 'WALLET_TRANSFER', 'MERCHANT_PAYMENT', 'WITHDRAWAL', 'REFUND'];
const channels   = ['MOBILE_APP', 'USSD', 'NFC'];
const banks      = ['CIB', 'NBE', 'QNB', 'BDC', 'MIB', 'HSBC', 'FAB', 'AGB'];
const statuses   = ['COMPLETED', 'FAILED', 'PENDING'];

function randomItem(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randomAmount(min, max) { return Math.round((Math.random() * (max - min) + min) * 100) / 100; }

// Generate a single batch of 10 events
const events = [];
for (let i = 0; i < 100; i++) {
    events.push({
        wallet_id: "WLT" + Math.floor(Math.random() * 100000).toString().padStart(8, '0'),
        event_type: randomItem(eventTypes),
        channel: randomItem(channels),
        issuing_bank_id: randomItem(banks),
        amount_egp: randomAmount(10, 5000),
        status: randomItem(statuses),
        event_ts: new Date(),
        _source_system: "bash_seed_script"
    });
}

// Insert data
const result = db.$COLLECTION_NAME.insertMany(events);
print("Success: " + result.acknowledged);
print("Inserted " + result.insertedIds.length + " documents into $DATABASE_NAME.$COLLECTION_NAME");

// Quick check of the count
print("Total documents in collection: " + db.$COLLECTION_NAME.countDocuments());
EOF

else
    echo "Error: Container $CONTAINER_NAME is not running."
    exit 1
fi