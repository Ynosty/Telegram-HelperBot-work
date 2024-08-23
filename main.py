from bs4 import BeautifulSoup
import requests
import pprint
from fake_useragent import UserAgent
import db
import time
import schedule
#Parser mod

class Parser_mod():

    def __init__(self) -> None:
        pass
    @property
    def headers(self) -> None:
        us = UserAgent()
        headers = {'User-Agent': str(us.chrome)}
        return headers
    
    #Parser rabota.ua 
    def rabota_ua(self,urls):
        try:
            # Create user agent headers (optional)
            headers = {'User-Agent': UserAgent().chrome}  # Consider moving outside if used elsewhere

            # Fetch data from the URL
            response = requests.get(url=urls, headers=headers)
            response.raise_for_status()

            # Parse JSON data
            data = response.json()

            # Check for "documents" key
            if "documents" in data:
                extracted_data = []
                for document in data["documents"]:
                    # Extract data from each document if necessary keys exist
                    if all(key in document for key in ('id', 'name', 'companyName', 'salary', 'salaryFrom', 'date')):
                        extracted_data.append({
                            "url": document["id"],
                            "name": document["name"]
                        })
                    else:
                        print(f"Missing required keys in document: {document}")
                return extracted_data
            else:
                print(f"Json response missing 'document' key: {data}")

            return None  # Indicate no data extracted

        except Exception as ex:
            print(f"Error fetching or parsing data: {ex}")
            return None


def scrape_and_store_data(start_page=1, end_page=5):
    pars = Parser_mod()
    for page_count in range(start_page, end_page + 1):
        url = f'https://ua-api.robota.ua/vacancy/search?page={page_count}&ukrainian=true&cityId=4&keyWords=it&noSalary=false&lastViewDate=true&sortBy=Date'
        data = pars.rabota_ua(url)
        if data:
            try:
                bd = db.parser_db()
                bd.create_rabota_ua()  # Create the table if it doesn't exist
                bd.insert_rabota(data)
            except Exception as e:
                print(f"Error connecting to or interacting with database: {e}")
        else:
            print(f"No data extracted from Robota.ua (page {page_count})")

schedule.every(10).minutes.do(scrape_and_store_data)

while True:
    schedule.run_pending()
    time.sleep(1)