#!/usr/bin/env python3
# observability/datahub/bootstrap_governance.py
# Registers domains, tags, and glossary terms in DataHub via REST API.
# No acryl-datahub SDK dependency — uses requests only.
import argparse, json, logging, sys
import requests

log = logging.getLogger(__name__)

DOMAINS = {
    "medallion_bronze":   {"name": "Bronze Layer",       "description": "Raw CDC events from PostgreSQL. Immutable Iceberg tables written by Kafka Sink."},
    "medallion_silver":   {"name": "Silver Layer",       "description": "Cleansed, deduplicated, PII-hashed. dbt incremental models."},
    "medallion_gold":     {"name": "Gold Layer",         "description": "Business aggregations. dbt full-refresh tables."},
    "medallion_serving":  {"name": "Serving Layer",      "description": "ClickHouse ReplacingMergeTree cache for BI."},
    "payment_rail_ach":   {"name": "EG-ACH Rail",        "description": "Egyptian ACH bank-to-bank transfers. CBE regulated."},
    "payment_rail_meeza": {"name": "Meeza Card Scheme",  "description": "National domestic debit card. ISO 8583."},
    "payment_rail_ipn":   {"name": "InstaPay Network",   "description": "Real-time P2P payments. CBE SLA <2 seconds."},
}

TAGS = {
    "PII":                   "Personally identifiable information",
    "PII-Hashed":            "PII hashed with SHA-256. Safe for analytics.",
    "PII-Tokenized":         "Tokenized at network level. PCI-DSS safe.",
    "PII-Proxy":             "Proxy identifier (mobile/IBAN alias).",
    "Payment-Card":          "Payment card data. PCI-DSS scope.",
    "Financial-Amount":      "Monetary amount in EGP.",
    "EGP":                   "Egyptian Pound denomination.",
    "KPI-Metric":            "CBE regulatory KPI.",
    "Bank-Identifier":       "CBE bank code.",
    "CDC-Metadata":          "Debezium metadata field (__op, __ts_ms).",
    "Source-of-Truth":       "Authoritative source. Do not modify directly.",
    "CDC-Enabled":           "Table streams CDC events via Debezium.",
    "Regulatory":            "Subject to CBE regulatory reporting.",
    "SLA-Critical":          "Subject to CBE SLA (IPN <2s, ACH T+0/T+1).",
    "Bronze":                "Medallion Bronze layer.",
    "Silver":                "Medallion Silver layer.",
    "Gold":                  "Medallion Gold layer.",
    "Serving":               "Medallion Serving layer.",
}

GLOSSARY_TERMS = {
    "PaymentSchemes/EG-ACH":            {"name": "EG-ACH",                 "definition": "Egyptian Automated Clearing House. Batch settlement. T+0/T+1 cycles.",           "termSource": "CBE Payment Systems Regulation 2022"},
    "PaymentSchemes/Meeza":             {"name": "Meeza",                   "definition": "Egypt national debit card scheme, ISO 8583 protocol.",                           "termSource": "CBE Meeza Circular"},
    "PaymentSchemes/InstaPay":          {"name": "InstaPay",                "definition": "Real-time IPN. Proxy-based P2P. CBE SLA ≤2 seconds.",                            "termSource": "CBE Instant Payment Regulation 2022"},
    "DataQuality/SilverGate":           {"name": "Silver Quality Gate",     "definition": "dbt tests: unique PK, non-null amount>0, valid status enums, CDC op validity.",   "termSource": "EBC Data Engineering Standards v2.0"},
    "DataQuality/IncrementalWatermark": {"name": "Incremental Watermark",   "definition": "max(_ingested_at) used to filter new Bronze records. Must use sink ingestion time not event time.", "termSource": "EBC Data Engineering Standards v2.0"},
    "Compliance/PAN-Tokenization":      {"name": "PAN Tokenization",        "definition": "Raw PAN never stored. card_token used. PCI-DSS Requirement 3.4.",                "termSource": "PCI-DSS v4.0"},
    "Compliance/IBAN-Hashing":          {"name": "IBAN Hashing",            "definition": "ACH IBANs SHA-256 hashed at Silver layer. CBE Customer Data Protection.",        "termSource": "CBE Circular 2021"},
    "Metrics/ApprovalRate":             {"name": "Approval Rate",           "definition": "approved_count / total_count × 100. Meeza ≥97%, IPN ≥99% (CBE baseline).",       "termSource": "CBE KPIs 2024"},
    "Metrics/IPNSLACompliance":         {"name": "IPN SLA Compliance",      "definition": "% of IPN txns with processing_time_ms ≤2000. <95% triggers CBE review.",         "termSource": "CBE Instant Payment Regulation Art.12"},
}


def ingest(session: requests.Session, gms_url: str, entity_type: str,
           entity_urn: str, aspect_name: str, aspect: dict, dry_run: bool) -> None:
    if dry_run:
        log.info("[DRY-RUN] %s  %s", aspect_name, entity_urn)
        return
    payload = {
        "proposal": {
            "entityType":  entity_type,
            "entityUrn":   entity_urn,
            "changeType":  "UPSERT",
            "aspectName":  aspect_name,
            "aspect": {
                "contentType": "application/json",
                "value":       json.dumps(aspect),
            },
        }
    }
    r = session.post(
        f"{gms_url}/aspects?action=ingestProposal",
        json=payload,
        timeout=30,
    )
    if not r.ok:
        log.warning("  %s %s → HTTP %s: %s", aspect_name, entity_urn, r.status_code, r.text[:200])
    r.raise_for_status()


def bootstrap(gms_url: str, dry_run: bool = False) -> None:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})

    if not dry_run:
        try:
            s.get(f"{gms_url}/health", timeout=10).raise_for_status()
        except Exception as e:
            log.error("GMS not reachable at %s: %s", gms_url, e)
            sys.exit(1)

    errors = 0

    for did, props in DOMAINS.items():
        try:
            ingest(s, gms_url, "domain", f"urn:li:domain:{did}",
                   "domainProperties",
                   {"name": props["name"], "description": props["description"]},
                   dry_run)
        except Exception as e:
            log.warning("domain %s failed: %s", did, e)
            errors += 1
    log.info("Domains: %d registered", len(DOMAINS))

    for tag, desc in TAGS.items():
        try:
            ingest(s, gms_url, "tag", f"urn:li:tag:{tag}",
                   "tagProperties", {"name": tag, "description": desc}, dry_run)
        except Exception as e:
            log.warning("tag %s failed: %s", tag, e)
            errors += 1
    log.info("Tags: %d registered", len(TAGS))

    nodes = sorted(set(t.split("/")[0] for t in GLOSSARY_TERMS))
    for node in nodes:
        try:
            ingest(s, gms_url, "glossaryNode", f"urn:li:glossaryNode:{node}",
                   "glossaryNodeInfo",
                   {"name": node, "definition": f"EBC {node} vocabulary"},
                   dry_run)
        except Exception as e:
            log.warning("glossaryNode %s failed: %s", node, e)
            errors += 1

    for path, props in GLOSSARY_TERMS.items():
        node = path.split("/")[0]
        try:
            ingest(s, gms_url, "glossaryTerm", f"urn:li:glossaryTerm:{path}",
                   "glossaryTermInfo",
                   {
                       "name":       props["name"],
                       "definition": props["definition"],
                       "termSource": props.get("termSource", "EBC Internal"),
                       "parentNode": f"urn:li:glossaryNode:{node}",
                   },
                   dry_run)
        except Exception as e:
            log.warning("glossaryTerm %s failed: %s", path, e)
            errors += 1
    log.info("Glossary terms: %d registered", len(GLOSSARY_TERMS))

    if errors:
        log.warning("Bootstrap completed with %d error(s) — check DataHub UI", errors)
        sys.exit(1)
    log.info("Bootstrap %s", "DRY RUN complete" if dry_run else "complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--gms-url", default="http://localhost:8082")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    bootstrap(gms_url=args.gms_url, dry_run=args.dry_run)
