import os
import logging
import pandas as pd
from iot_analytics.mongo import get_db

logger = logging.getLogger(__name__)

db = get_db()
collection = db["sales"]

LOG_DIR = os.path.join("media", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
SALES_LOG_PATH = os.path.join(LOG_DIR, "sales_analysis.log")

# Dedicated file handler for sales operations
sales_file_handler = logging.FileHandler(SALES_LOG_PATH)
sales_file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
sales_file_handler.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', '') == os.path.abspath(SALES_LOG_PATH) for h in logger.handlers):
    logger.addHandler(sales_file_handler)
logger.setLevel(logging.INFO)

def ingest_data(csv_path: str):
    """
    Ingest CSV into 'sales' collection. Dedupe by Order ID and Product ID.
    """
    try:
        df = pd.read_csv(csv_path)
        df["unique_key"] = list(zip(df["Order ID"], df["Product ID"]))
        existing = set((x["Order ID"], x["Product ID"]) for x in collection.find({}, {"Order ID": 1, "Product ID": 1, "_id": 0}))
        new_df = df[~df["unique_key"].isin(existing)].drop(columns=["unique_key"])
        records = new_df.to_dict(orient="records")
        if records:
            collection.insert_many(records)
            logger.info("Inserted %d in collection %s", len(records), collection.name)
            return {"inserted": len(records)}
        else:
            logger.info("No new entries to insert.")
            return {"inserted": 0}
    except Exception:
        logger.exception("Exception occurred in ingest_data")
        raise

def top_five():
    """
    Calculates the top 5 products (Product ID) with the highest total Sales.
    """
    try:
        pipeline = [
            {"$group": {"_id": "$Product ID", "gross_sale": {"$sum": "$Sales"}}},
            {"$sort": {"gross_sale": -1}},
            {"$limit": 5},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("Displayed top 5 products.")
        return rec
    except Exception:
        logger.exception("Exception in top_five")
        raise

def monthly_revenue():
    """
    Calculates the monthly revenue for each year.
    """
    try:
        pipeline = [
            {"$set": {"month_year": {"$dateToString": {"format": "%Y-%m", "date": {"$dateFromString": {"dateString": "$Order Date", "format": "%d/%m/%Y"}}}}}},
            {"$group": {"_id": "$month_year", "monthly_revenue": {"$sum": "$Sales"}}},
            {"$sort": {"_id": 1}},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("Displayed monthly revenue.")
        return rec
    except Exception:
        logger.exception("Exception in monthly_revenue")
        raise

def avg_sales():
    """
    Calculates average sales per sub-category, grouped by category.
    """
    try:
        pipeline = [
            {"$group": {"_id": {"category": "$Category", "subcategory": "$Sub-Category"}, "avg_sales": {"$avg": "$Sales"}}},
            {"$group": {"_id": "$_id.category", "sub-category": {"$push": {"SC": "$_id.subcategory", "Avg_Sales": "$avg_sales"}}}},
        ]
        rec = list(collection.aggregate(pipeline))
        logger.info("Displayed average sales by category and sub-category.")
        return rec
    except Exception:
        logger.exception("Exception in avg_sales")
        raise

def annual_growth():
    """
    Calculates the annual growth of total sales.
    """
    try:
        pipeline = [
            {"$set": {"year": {"$dateToString": {"format": "%Y", "date": {"$dateFromString": {"dateString": "$Order Date", "format": "%d/%m/%Y"}}}}}},
            {"$group": {"_id": "$year", "total_sales": {"$sum": "$Sales"}}},
            {"$sort": {"_id": 1}},
        ]
        rec = list(collection.aggregate(pipeline))
        out = []
        prev = None
        for doc in rec:
            year = doc["_id"]
            sales = doc["total_sales"]
            if prev is None:
                out.append({"year": year, "total_sales": round(sales, 2), "growth_pct": None})
            else:
                growth = ((sales - prev) / prev) * 100 if prev != 0 else 0
                out.append({"year": year, "total_sales": round(sales, 2), "growth_pct": round(growth, 2)})
            prev = sales
        logger.info("Displayed annual growth.")
        return out
    except Exception:
        logger.exception("Exception in annual_growth")
        raise
