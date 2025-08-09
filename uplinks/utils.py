import os
import json
import logging
import pandas as pd
from iot_analytics.mongo import get_db

logger = logging.getLogger(__name__)

db = get_db()
collection = db["uplinks"]

LOG_DIR = os.path.join("media", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
UPLINKS_LOG_PATH = os.path.join(LOG_DIR, "uplinks_analysis.log")

# Dedicated file handler for uplinks operations
uplinks_file_handler = logging.FileHandler(UPLINKS_LOG_PATH)
uplinks_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
uplinks_file_handler.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == os.path.abspath(UPLINKS_LOG_PATH) for h in logger.handlers):
    logger.addHandler(uplinks_file_handler)
logger.setLevel(logging.INFO)

def ingest_data(csv_path: str):
    """
    Ingest CSV into 'uplinks' collection. Dedupe by dev_eui.
    """
    try:
        df = pd.read_csv(csv_path)
        existing = set(x.get("dev_eui") for x in collection.find({}, {"dev_eui": 1, "_id": 0}))
        new_df = df[~df["dev_eui"].isin(existing)]
        records = new_df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
            logger.info("Inserted %d new entries into collection %s.", len(records), collection.name)
            return {"inserted": len(records)}
        else:
            logger.info("No new entries to insert.")
            return {"inserted": 0}
    except Exception:
        logger.exception("Exception occurred in ingest_data")
        raise

def highest_uplinks(n: int):
    """
    Calculates the n number of devices with highest uplinks.
    """
    try:
        pipeline = [
            {"$group": {"_id": "$device_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": int(n)},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("Fetched top %d devices with highest uplinks.", n)
        return rec
    except Exception:
        logger.exception("Exception occurred in highest_uplinks")
        raise

def avg_rssi_snr():
    """
    Calculates average rssi and snr for each device.
    """
    try:
        pipeline = [
            {"$group": {"_id": "$device_id", "avg_rssi": {"$avg": "$rssi"}, "avg_snr": {"$avg": "$snr"}}},
            {"$sort": {"avg_rssi": 1, "avg_snr": 1}},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("%d unique devices found, avg rssi and snr calculated.", len(rec))
        return rec
    except Exception:
        logger.exception("Exception occurred in avg_rssi_snr")
        raise

def avg_weather():
    """
    Calculates average temperature and humidity for each gateway_id.
    """
    try:
        pipeline = [
            {"$group": {"_id": "$gateway_id", "avg_temp": {"$avg": "$temperature"}, "avg_humidity": {"$avg": "$humidity"}}},
            {"$sort": {"avg_temp": 1}},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("%d total records after getting avg temperature and humidity for each gateway_id.", len(rec))
        return rec
    except Exception:
        logger.exception("Exception occurred in avg_weather")
        raise

def get_duplicates():
    """
    Returns device_ids with duplicate documents.    
    """
    try:
        pipeline = [
            {"$group": {"_id": "$device_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": 2}}},
            {"$sort": {"count": -1, "_id": 1}},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("There are %d device_ids with duplicate documents.", len(rec))
        return rec
    except Exception:
        logger.exception("Exception occurred in get_duplicates")
        raise

def export_hot_temps(output_path: str):
    """
    Exports documents with temperature > 35.
    """
    try:
        docs = collection.find(
            {"temperature": {"$gt": 35}},
            {"_id": 0, "device_id": 1, "latitude": 1, "longitude": 1, "temperature": 1},
        )
        rec = list(docs)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(rec, f, indent=4)
        logger.info("%d documents exported to %s", len(rec), output_path)
        return {"exported": len(rec), "path": output_path}
    except Exception:
        logger.exception("Exception occurred in export_hot_temps")
        raise
