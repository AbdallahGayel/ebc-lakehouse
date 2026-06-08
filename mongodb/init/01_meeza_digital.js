// =============================================================================
// mongodb/init/01_meeza_digital.js
// Meeza Digital Wallet — event store for mobile wallet transactions
// Executed by MongoDB on first startup via /docker-entrypoint-initdb.d
//
// WHY MONGODB:
//   Meeza Digital wallet events carry variable-schema payloads per event type
//   (top-up has funding_source, QR payment has merchant_id/vat_amount, etc).
//   A document store is the natural fit. Debezium MongoDB connector reads
//   change events from the replica set oplog.
//
// NOTE: This script runs in mongosh (not Node.js).
//   - require('crypto') is NOT available — use UUID().toString() for tokens
//   - db object is already connected; use db.getSiblingDB() to switch DBs
// =============================================================================

// Give the replica set election time to complete before seeding
sleep(3000);

db = db.getSiblingDB('meeza_digital');

// ── Collection: wallet_events ─────────────────────────────────────────────────
db.createCollection('wallet_events');

db.wallet_events.createIndex({ wallet_id: 1 });
db.wallet_events.createIndex({ event_ts: -1 });
db.wallet_events.createIndex({ event_type: 1, event_ts: -1 });
db.wallet_events.createIndex({ linked_card_token: 1 });

// ── Helper functions (mongosh-compatible — no require()) ──────────────────────
const eventTypes = ['TOP_UP', 'QR_PAYMENT', 'WALLET_TRANSFER', 'MERCHANT_PAYMENT', 'WITHDRAWAL', 'REFUND'];
const channels   = ['MOBILE_APP', 'USSD', 'NFC'];
const banks      = ['CIB', 'NBE', 'QNB', 'BDC', 'MIB', 'HSBC', 'FAB', 'AGB'];
const statuses   = ['COMPLETED', 'COMPLETED', 'COMPLETED', 'FAILED', 'PENDING'];
const merchants  = ['CARREFOUR', 'SEOUDI', 'B_TECH', 'VODAFONE', 'ETISALAT', 'WE_TELECOM', 'METRO_MARKET'];

function randomItem(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randomAmount(min, max) { return Math.round((Math.random() * (max - min) + min) * 100) / 100; }
function daysAgo(maxDays) { return new Date(Date.now() - Math.random() * maxDays * 86400000); }

// mongosh-compatible hex token: use UUID().toString() and strip dashes
function randomHexToken() {
    return UUID().toString().replace(/-/g, '').replace('UUID("', '').replace('")', '');
}

function walletId() {
    return 'WLT' + String(Math.floor(Math.random() * 99999)).padStart(8, '0');
}

// ── Seed 500 wallet events ────────────────────────────────────────────────────
const events = [];
for (let i = 0; i < 500; i++) {
    const eventType = randomItem(eventTypes);
    const base = {
        wallet_id:         walletId(),
        linked_card_token: randomHexToken(),   // mongosh-safe token generation
        issuing_bank_id:   randomItem(banks),
        event_type:        eventType,
        channel:           randomItem(channels),
        status:            randomItem(statuses),
        currency_code:     'EGP',
        event_ts:          daysAgo(7),
        _source_system:    'meeza_digital',
    };

    // Variable payload per event type (why MongoDB is used — variable schema)
    if (eventType === 'TOP_UP') {
        base.amount_egp     = randomAmount(100, 5000);
        base.funding_source = randomItem(['BANK_TRANSFER', 'DEBIT_CARD', 'AGENT']);
        base.bank_ref       = 'TR' + String(i).padStart(10, '0');

    } else if (eventType === 'QR_PAYMENT' || eventType === 'MERCHANT_PAYMENT') {
        base.amount_egp     = randomAmount(10, 2000);
        base.merchant_id    = 'MER' + String(Math.floor(Math.random() * 99999)).padStart(8, '0');
        base.merchant_name  = randomItem(merchants);
        base.qr_ref         = 'QR' + String(i).padStart(12, '0');
        base.vat_amount     = Math.round(base.amount_egp * 0.14 * 100) / 100;

    } else if (eventType === 'WALLET_TRANSFER') {
        base.amount_egp          = randomAmount(50, 10000);
        base.receiver_wallet_id  = walletId();
        base.transfer_note       = 'Transfer ' + i;

    } else if (eventType === 'WITHDRAWAL') {
        base.amount_egp  = randomAmount(200, 3000);
        base.atm_id      = 'ATM' + String(Math.floor(Math.random() * 9999)).padStart(6, '0');
        base.location    = randomItem(['CAIRO', 'ALEX', 'GIZA', 'MANSOURA']);

    } else if (eventType === 'REFUND') {
        base.amount_egp    = randomAmount(10, 500);
        base.original_ref  = 'QR' + String(Math.floor(Math.random() * 999999)).padStart(12, '0');
        base.refund_reason = randomItem(['MERCHANT_REQUEST', 'CUSTOMER_DISPUTE', 'TECHNICAL_ERROR']);
    }

    events.push(base);
}

db.wallet_events.insertMany(events);
print('[initdb] meeza_digital.wallet_events: seeded ' + db.wallet_events.countDocuments() + ' documents');
