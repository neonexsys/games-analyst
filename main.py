import logging
import os
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import FileResponse
from pandas import json_normalize

import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
from src.db import MongoDB


from src.gematsu_scraper import GematsuScraper
from src.metacritic_scraper import MetacriticScraper
from pytz import timezone


# Create a custom logger
logger = logging.getLogger(__name__)

# Set the level of this logger. INFO means that all the logs of level INFO and above will be tracked
logger.setLevel(logging.INFO)

# Create handlers
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
c_format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)


app = FastAPI()
# print the current working directory


@app.get("/api/v1/scrape-metacritic", tags=["Scraping"])
def scrape_metacritic():
  """
  This endpoint initiates the scraping process for Metacritic data. 
  It creates a new instance of the MetacriticScraper, calls the scrape method to scrape data, 
  writes the scraped data to a CSV file, and then writes the data to the MongoDB database.
  """
  logger.info("Scraping started.")
  scraper = MetacriticScraper()
  scraper.scrape()
  scraper.write_to_csv()
  scraper.write_to_mongodb()
  logger.info("Scraping completed and data written to MongoDB.")
  return {"message": "Scraping completed and data written to MongoDB."}


@app.get("/api/v1/scrape-gematsu", tags=["Scraping"])
def scrape_gematsu():
  """
  This endpoint initiates the scraping process for Gematsu data. 
  It creates a new instance of the GematsuScraper, calls the scrape method to scrape data, 
  and then writes the scraped data to the MongoDB database.
  """
  logger.info("Gematsu scraping started.")
  scraper = GematsuScraper()
  scraper.scrape()
  scraper.write_to_mongodb()
  logger.info("Gematsu scraping completed and data written to MongoDB.")
  return {"message": "Gematsu scraping completed and data written to MongoDB."}


@app.get(
  "/api/v1/metacritic-data", response_class=FileResponse, tags=["Data Retrieval"]
)
def get_data():
  """
  This endpoint retrieves Metacritic data from the MongoDB database, 
  converts the data to a pandas DataFrame, and then writes the DataFrame to a CSV file. 
  The CSV file is then returned as a response.
  """
  logger.info("Data retrieval started.")
  scraper = MetacriticScraper()

  try:
    # check if the 'metacritic_scores' collection exists
    if scraper.collection.count_documents({}) == 0:
      logger.error("No Metacritic data found in the database.")
      return {"message": "No Metacritic data found in the database."}
  except Exception as e:
    logger.error("No Metacritic data found in the database.")
    return {"message": "No Metacritic data found in the database."}

  data = list(scraper.collection.find())
  df = pd.DataFrame(data)

  # Get the current date as 'YYYY-MM-DD'
  date_str = datetime.now().strftime("%Y-%m-%d")

  # Write the DataFrame to a CSV file
  filename = f"export_metacritic_data_{date_str}.csv"
  df.to_csv(filename, index=False)

  logger.info("Data retrieval completed.")
  return FileResponse(filename, media_type="text/csv", filename=filename)


@app.get("/api/v1/gematsu-data", tags=["Data Retrieval"])
def get_gematsu_data():
  """
  This endpoint retrieves Gematsu data from the MongoDB database, 
  flattens the sales data and hardware sales data, converts the data to pandas DataFrames, 
  and then writes the DataFrames to an Excel file with two tabs. 
  The Excel file is then returned as a response.
  """
  logger.info("Gematsu data retrieval started.")
  scraper = GematsuScraper()

  # try:
  #   # check if there is any gematsu_data in the gamesanalyst database. If not, return 404 no data found
  #   # the database is called gamesanalyst and the collection is called gematsu_data
  #   if scraper.collection["gematsu_data"].count_documents({}) == 0:
  #     logger.error("No Gematsu data found in the database.")
  #     return {"message": "No Gematsu data found in the database."}
  # except Exception as e:
  #   logger.error("No Gematsu data found in the database.")
  #   return {"message": "No Gematsu data found in the database."}

  # Get the data from the MongoDB database
  data = list(scraper.collection.find())

  # Flatten the sales data and hardware sales data
  sales_data = json_normalize(data, "sales_data", ["link", "start_date", "end_date"])
  hardware_sales_data = json_normalize(
    data, "hardware_sales_data", ["link", "start_date", "end_date"]
  )

  # Get the current date as 'YYYY-MM-DD'
  date_str = datetime.now().strftime("%Y-%m-%d")

  # Write the DataFrame to a CSV file
  filename = f"export_gematsu_data_{date_str}.xlsx"

  # Write the DataFrames to an Excel file with two tabs
  with pd.ExcelWriter(filename) as writer:
    sales_data.to_excel(writer, sheet_name="Sales Data", index=False)
    hardware_sales_data.to_excel(writer, sheet_name="Hardware Sales Data", index=False)

  logger.info("Gematsu data retrieval completed.")
  return FileResponse(
    filename,
    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    filename=filename,
  )

@app.get("/api/v1/get-latest-data", tags=["Data Retrieval"])
def get_combined_data():
  """
  This endpoint retrieves the latest data from the 'gematsu_data' and 'metacritic_scores' collections in the MongoDB database.
  It then flattens the 'sales_data' list of dictionaries in the 'gematsu_data' and converts it to a pandas DataFrame.
  And then it merges the two DataFrames on the game name so that the metacritic data is added to the gematsu data.
  """
  mongo = MongoDB()
  db = mongo.get_db()

  # Get the date one month ago
  three_months_ago = datetime.now() - timedelta(days=90)

  # Query the last month's worth of data from the 'gematsu_data' collection
  gematsu_data = list(db["gematsu_data"].find({"end_date": {"$gte": three_months_ago}}))

  # Query all data from the 'metacritic_data' collection
  metacritic_data = list(db["metacritic_scores"].find())

  # the gematsu_data contains a list of dictionaries in the 'sales_data', so we need to flatten it
  gematsu_sales_data = json_normalize(gematsu_data, "sales_data", ["link", "start_date", "end_date"])

  # convert gematsu_sales_data to a pandas DataFrame
  # Convert the queried data to pandas DataFrames
  gematsu_df = pd.DataFrame(gematsu_sales_data)
  metacritic_df = pd.DataFrame(metacritic_data)

  # rename the 'release_date' column to 'release_date_gematsu' in the gematsu_df
  gematsu_df.rename(columns={"release_date": "release_date_gematsu"}, inplace=True)

  # rename the 'release_date' column to 'release_date_metacritic' in the metacritic_df
  metacritic_df.rename(columns={"release_date": "release_date_metacritic"}, inplace=True)


  # Merge the two DataFrames on the game name using a left join
  combined_df = pd.merge(gematsu_df, metacritic_df, left_on="game_title", right_on="title", how="left")

  # columns to keep
  # platform, game_title, company, release_date_gematsu, start_date, end_date, weekly_sales, total_sales, metascore, rating, release_date_metacritic
  combined_df = combined_df[["platform", "game_title", "company", "release_date_gematsu", "start_date", "end_date", "weekly_sales", "total_sales", "metascore", "rating", "release_date_metacritic"]]

  # make the date columns into datetime objects and then change back into string in the format 'YYYY-MM-DD'
  combined_df["release_date_gematsu"] = pd.to_datetime(combined_df["release_date_gematsu"]).dt.strftime("%Y-%m-%d")
  combined_df["start_date"] = pd.to_datetime(combined_df["start_date"]).dt.strftime("%Y-%m-%d")
  combined_df["end_date"] = pd.to_datetime(combined_df["end_date"]).dt.strftime("%Y-%m-%d")
  combined_df["release_date_metacritic"] = pd.to_datetime(combined_df["release_date_metacritic"]).dt.strftime("%Y-%m-%d")

  # order the combined_df by the 'end_date' column in descending order
  combined_df = combined_df.sort_values(by="end_date", ascending=False)

  # Export the merged DataFrame to an Excel file
  tokyo_tz = timezone('Asia/Tokyo')
  today_dt = datetime.now(tokyo_tz).strftime("%Y-%m-%d")
  file_name = f"combined_data_{today_dt}.xlsx"
  combined_df.to_excel(file_name, index=False)

  return FileResponse(
    file_name,
    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    filename=file_name,
  )

@app.delete("/api/v1/clear-database", tags=["Data Management"])
def clear_data():
    """
    This endpoint deletes all documents from the 'gematsu_data' and 'metacritic_scores' collections in the MongoDB database.
    """
    mongo = MongoDB()
    db = mongo.get_db()

    # Delete all documents from the 'gematsu_data' collection
    db["gematsu_data"].delete_many({})

    # Delete all documents from the 'metacritic_scores' collection
    db["metacritic_scores"].delete_many({})

    return {"message": "Data cleared from 'gematsu_data' and 'metacritic_scores' collections."}