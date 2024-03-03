from fastapi import FastAPI
from metacritic_scraper import MetacriticScraper
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import logging


# Create a custom logger
logger = logging.getLogger(__name__)

# Set the level of this logger. INFO means that all the logs of level INFO and above will be tracked
logger.setLevel(logging.INFO)

# Create handlers
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)


app = FastAPI()

@app.get("/api/v1/scrape-metacritic")
def scrape_metacritic():
    logger.info("Scraping started.")
    scraper = MetacriticScraper()
    scraper.scrape()
    scraper.write_to_csv()
    scraper.write_to_mongodb()
    logger.info("Scraping completed and data written to MongoDB.")
    return {"message": "Scraping completed and data written to MongoDB."}



@app.get("/api/v1/data", response_class=StreamingResponse)
def get_data():
    logger.info("Data retrieval started.")
    scraper = MetacriticScraper()
    data = list(scraper.collection.find())
    df = pd.DataFrame(data)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    logger.info("Data retrieval completed.")
    return response