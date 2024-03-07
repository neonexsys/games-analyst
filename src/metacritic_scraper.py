import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv

from src.db import MongoDB



class MetacriticScraper:
  def __init__(self):
    thisyear = datetime.now().year
    twoyearsago = thisyear - 1
    print(f"Scraping games released between {twoyearsago} and {thisyear}...")
    self.base_url = f"https://www.metacritic.com/browse/game/?releaseYearMin={twoyearsago}&releaseYearMax={thisyear}&platform=ps5&platform=xbox-series-x&platform=nintendo-switch&platform=pc&platform=mobile&platform=3ds&page="
    print("Base URL: ", self.base_url)
    self.session = requests.Session()
    self.session.headers.update(
      {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"
      }
    )
    self.games_data = []

    self.db = MongoDB(collection_name='metacritic_scores')
    self.collection = self.db.collection


  def scrape(self):
    page:int = 1
    hybernate:bool = False
    while True:
      print(f"\n>>> Scraping page {page}...")
      response = self.session.get(self.base_url + str(page))
      soup = BeautifulSoup(response.content, "html.parser")
      game_items = soup.find_all(
        "div", class_="c-finderProductCard c-finderProductCard-game"
      )

      if not game_items:
        print("No more game items found. Exiting...")
        if hybernate is False:
          hybernate = True
          print("Hybernating for 300 seconds...")
          time.sleep(300)
          continue
        else:
          break
      for game_item in game_items:
        self.extract_game_data(game_item, page)

      page += 1
      # wait 500ms before scraping the next page
      time.sleep(30)


  def extract_game_data(self, game_item, pagenum:int):
    title = game_item.find("h3", class_="c-finderProductCard_titleHeading").text.strip()
    title = title.split(". ", 1)[1]  # Remove the number from the start of the title

    release_date = game_item.find("div", class_="c-finderProductCard_meta").text.strip()
    release_date_tokens = release_date.split("\n")
    release_date = release_date_tokens[0]  # Extract only the date part
    release_date = datetime.strptime(release_date, "%b %d, %Y").strftime(
      "%Y-%m-%d"
    )  # Convert to YYYY-MM-DD format

    try:
      rating = release_date_tokens[len(release_date_tokens) - 1].split("Rated ")[
        1
      ]  # Extract the rating
    except Exception as e:
      rating = "N/A"
      print(f"An error occurred while getting the rating for [{title}], [{release_date}], [page:{pagenum}]: {e}")

    try:
      metascore = game_item.find(
        "div",
        class_="c-siteReviewScore u-flexbox-column u-flexbox-alignCenter u-flexbox-justifyCenter g-text-bold c-siteReviewScore_green g-color-gray90 c-siteReviewScore_xsmall",
      ).text.strip()
    except Exception as e:
      metascore = "N/A"
      print(f"An error occurred while getting the metascore: {e}")

    self.games_data.append(
      {
        "title": title,
        "release_date": release_date,
        "rating": rating,
        "metascore": metascore,
      }
    )

  def write_to_csv(self):
    today_str = datetime.today().strftime("%Y-%m-%d")
    file_name = f"games_data_{today_str}.csv"
    with open(file_name, "w", newline="") as csvfile:
      fieldnames = ["title", "release_date", "rating", "metascore"]
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

      writer.writeheader()
      for game in self.games_data:
        writer.writerow(game)

    print(f"The games data has been successfully written to '{file_name}'.")

  def write_to_mongodb(self):
    for game in self.games_data:
      print(f"writing game: {game}")
      self.collection.update_one(
        {"title": game["title"]},  # query
        {"$set": game},  # new data
        upsert=True,  # create a new document if no document matches the query
      )
    print("The games data has been successfully written to MongoDB.")


if __name__ == "__main__":
  scraper = MetacriticScraper()
  scraper.scrape()
  scraper.write_to_csv()
  scraper.write_to_mongodb()

