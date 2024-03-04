# Import necessary libraries
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import requests
import re

from src.db import MongoDB


class GematsuScraper:
  def __init__(self):
    # Initialize base URL, requests session, and MongoDB collection
    self.base_url = "https://www.gematsu.com/tag/famitsu-sales"
    self.session = requests.Session()
    self.session.headers.update(
      {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1"
      }
    )
    self.games_sales_data = []
    self.hardware_sales_data = []

    self.mongo = MongoDB(collection_name="gematsu_data")
    self.db = self.mongo.db
    self.collection = self.mongo.collection

  def get_existing_entries(
    self, link: str, start_date: datetime, end_date: datetime
  ) -> tuple[list, list]:
    """_summary_

    Args:
        link (str): _description_
        start_date (datetime): _description_
        end_date (datetime): _description_

    Returns:
        a list of dictionaries containing the sales data for each game
    """

    # Send a GET request to the link
    response = self.session.get(link)
    html = response.content.decode("utf-8").replace("&nbsp;", " ")
    soup = BeautifulSoup(html, "html.parser")

    # Replace non-breaking spaces with regular spaces
    for text in soup.find_all(string=True):
      if "\xa0" in text:
        text.replace_with(text.replace("\xa0", " "))

    # Find the <ol> tag that comes after a <p> tag containing <strong>Software Sales</strong>
    sales_ols = [
      ol
      for ol in soup.select(".post__content-main ol")
      if "display: none" not in ol.get("style", "")
    ]
    software_sales_ol = sales_ols[0] if sales_ols else None
    hardware_sales_ol = sales_ols[1] if len(sales_ols) > 1 else None

    # Create a list to store the sales data for each game
    sales_data_list = []
    hardware_sales_data_list = []

    # Loop through each <li> item in the <ol> tag
    for li in software_sales_ol.find_all("li"):
      # Extract the platform
      platform = li.text.split(" ")[0].replace("[", "").replace("]", "")

      # Extract the game title
      game_title = li.em.text

      # Extract the company
      company_match = re.search(r"\((.*?)(?:,|\))", li.text)
      company = company_match.group(1) if company_match else None

      # Extract the release date
      date_match = re.search(r"\d{1,2}/\d{1,2}/\d{2}", str(li.contents))
      release_date = (
        datetime.strptime(date_match.group(), "%m/%d/%y") if date_match else None
      )

      # if the last element text contains New then use li.contents[-2] else use li.contents[-1]
      is_new = False
      if "New" in str(li.contents[-1]):
        # Extract the weekly sales and total sales
        is_new = True
        sales_match = re.search(
            r"–\s*(\d{1,3}(,\d{3})*)(?:\s*\((?:<strong>)?(New|\d{1,3}(,\d{2,3})*)(?:<\/strong>)?\))?",
            str(li.contents[-2]),
        )
      else:
        # Extract the weekly sales and total sales
        sales_match = re.search(
            r"–\s*(\d{1,3}(,\d{3})*)(?:\s*\((?:<strong>)?(New|\d{1,3}(,\d{2,3})*)(?:<\/strong>)?\))?",
            str(li.contents[-1]),
        )
      
      weekly_sales = sales_match.group(1).replace(",", "") if sales_match else None
      if is_new:
          lifetime_sales = weekly_sales
      else:
          lifetime_sales = (
          sales_match.group(3).replace(",", "")
          if sales_match and sales_match.group(3) and sales_match.group(3) != "New"
          else None
          )

      # Add the sales data to the list
      sales_data_list.append(
        {
          "platform": platform,
          "game_title": game_title,
          "company": company,
          "release_date": release_date,
          "weekly_sales": int(weekly_sales) if weekly_sales else None,
          "total_sales": int(lifetime_sales) if lifetime_sales else None,
        }
      )

    for li in hardware_sales_ol.find_all("li"):
      # Split the string on '–' to separate the platform from the sales numbers
      parts = li.text.split("–")
      platform = parts[0].strip()

      # Extract the weekly and lifetime sales
      sales_match = re.search(
        r"(\d{1,3}(,\d{3})*)(?:\s*\((\d{1,3}(,\d{3})*|New)\))?", parts[1]
      )
      weekly_sales = sales_match.group(1).replace(",", "") if sales_match else None
      lifetime_sales = (
        sales_match.group(3).replace(",", "")
        if sales_match and sales_match.group(3) != "New"
        else None
      )

      # Add the data to the list
      hardware_sales_data_list.append(
        {
          "platform": platform,
          "weekly_sales": int(weekly_sales) if weekly_sales else None,
          "lifetime_sales": int(lifetime_sales) if lifetime_sales else None,
        }
      )
    return sales_data_list, hardware_sales_data_list

  def parse_page(self, soup):
    # Find all the <article> tags with class 'gematsu-post'
    articles = soup.select(".gematsu-listing--famitsu-sales article.gematsu-post")

    # Loop through each article
    for article in articles:
      # Find the <h2> tag and extract the link and date range
      h2_tag = article.find("h2")
      link = h2_tag.a["href"]
      date_range = h2_tag.a.text.split(" [", 1)[0].replace("Famitsu Sales: ", "")

      # Split the date range into start date and end date
      start_date_str, end_date_str = date_range.split(" – ")

      # Parse the start date and end date into Python dates
      start_date = datetime.strptime(start_date_str, "%m/%d/%y")
      end_date = datetime.strptime(end_date_str, "%m/%d/%y")

      # If the title matches the pattern "Famitsu Sales: DD/MM/YY – DD/MM/YY" and is not already in the database, navigate to the detail page, otherwise skip

      if (
        self.collection.find_one(
          {"start_date": start_date, "end_date": end_date, "link": link}
        )
        is None
      ):
        sale_data_list, hardware_sales_data_list = self.get_existing_entries(
          link, start_date, end_date
        )
        if sale_data_list:
          self.games_sales_data.append(
            {
              "link": link,
              "start_date": start_date,
              "end_date": end_date,
              "sales_data": sale_data_list,
            }
          )
        if hardware_sales_data_list:
          self.hardware_sales_data.append(
            {
              "link": link,
              "start_date": start_date,
              "end_date": end_date,
              "hardware_sales_data": hardware_sales_data_list,
            }
          )

  def scrape(self, run_all_pages=False):
    # Get the HTML content of the landing page
    response = self.session.get(self.base_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Parse the top page
    self.parse_page(soup)

    if run_all_pages:
      # Find the last page number
      pagination = soup.find("div", class_="gematsu-pagination")
      last_page = int(pagination.find_all("a", class_="page-numbers")[-2].text)

      # Loop through all the pages
      for page in range(2, last_page + 1):
        page_url = f"{self.base_url}/page/{page}"

        # Get the HTML content of the page
        response = self.session.get(page_url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Parse the page
        self.parse_page(soup)

    return self.games_sales_data

  def write_to_mongodb(self):
    # Get the current date and time
    update_timestamp = datetime.now()

    # Loop through the games_data list
    for game in self.games_sales_data:
      # Extract the data
      link = game["link"]
      start_date = game["start_date"]
      end_date = game["end_date"]
      sales_data_list = game["sales_data"]

      # Find the corresponding hardware sales data
      hardware_sales_data_list = next(
        (
          hardware["hardware_sales_data"]
          for hardware in self.hardware_sales_data
          if hardware["start_date"] == start_date and hardware["end_date"] == end_date
        ),
        None,
      )

      # Save the data to the MongoDB collection
      self.collection.insert_one(
        {
          "link": link,
          "start_date": start_date,
          "end_date": end_date,
          "sales_data": sales_data_list,
          "hardware_sales_data": hardware_sales_data_list,
          "update_timestamp": update_timestamp,
        }
      )

  def write_to_excel(self):
    # Create a Pandas DataFrame from the games_data list
    flat_data = [
      {
        "link": item["link"],
        "start_date": item["start_date"],
        "end_date": item["end_date"],
        **sale,
      }
      for item in self.games_sales_data
      for sale in item["sales_data"]
    ]
    df = pd.DataFrame(flat_data)

    # Create a Pandas DataFrame from the hardware_sales_data list
    flat_data_hardware = [
      {
        "link": item["link"],
        "start_date": item["start_date"],
        "end_date": item["end_date"],
        **sale,
      }
      for item in self.hardware_sales_data
      for sale in item["hardware_sales_data"]
    ]
    df_hardware = pd.DataFrame(flat_data_hardware)

    # Check if the dataframes are not empty before writing to excel
    if not df.empty:
      # Get the current date in YYYY-MM-DD format
      today_str = datetime.today().strftime("%Y-%m-%d")

      # Create an Excel file with the current date in the filename
      file_name = f"gematsu_data_{today_str}.xlsx"
      with pd.ExcelWriter(file_name) as writer:
        df.to_excel(writer, sheet_name="Software Sales", index=False)
        if not df_hardware.empty:
          df_hardware.to_excel(writer, sheet_name="Hardware Sales", index=False)

      print(f"Data written to {file_name}.")
    else:
      print("No data to write.")

  def write_to_csv(self):
    # Create a Pandas DataFrame from the games_data list
    # first flatten the list of dictionaries so that each dictionary is a row in the dataframe and each software sale is a row with the same link, start_date, and end_date
    flat_data = [
      {
        "link": item["link"],
        "start_date": item["start_date"],
        "end_date": item["end_date"],
        **sale,
      }
      for item in self.games_sales_data
      for sale in item["sales_data"]
    ]

    df = pd.DataFrame(flat_data)

    # Get the current date in YYYY-MM-DD format
    today_str = datetime.today().strftime("%Y-%m-%d")

    # Create a CSV file with the current date in the filename
    file_name = f"gematsu_data_{today_str}.csv"
    df.to_csv(file_name, index=False)
    print(f"Data written to {file_name}.")


if __name__ == "__main__":
  # Create an instance of GematsuScraper
  scraper = GematsuScraper()
  # Call the scrape method to start the scraping process
  scraper.scrape(run_all_pages=False)

  # Call the write_to_mongodb method to save the scraped data to the database
  scraper.write_to_mongodb()

  # Call the write_to_csv method to save the scraped data to a CSV file
  scraper.write_to_excel()
