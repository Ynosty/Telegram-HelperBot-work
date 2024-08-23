import threading 
import time
import schedule
from db import parser_db
from main import Parser_mod
from gjini import Djini_parser

def scrapy_rabota(start_page, end_page):
    pars = Parser_mod()
    for page_count in range(start_page, end_page + 1):
        url = f'https://ua-api.robota.ua/vacancy/search?page={page_count}&ukrainian=true&cityId=4&keyWords=hr&noSalary=false&lastViewDate=true&sortBy=Date'
        data = pars.rabota_ua(url)
        if data:
            try:
                conn = parser_db.connect_to_db()
                parser_db.create_table(conn)  # Create the table if it doesn't exist
                parser_db.insert_data(conn, data)
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Error connecting to or interacting with database: {e}")
        else:
            print(f"No data extracted from Robota.ua (page {page_count})")


def scrape_djini(url):
    parser = Djini_parser(url)
    data = parser.parser()
    conn = parser_db.connect_to_db()
    parser_db.create_djini(conn)
    parser_db.insert_djini(conn, data)
    conn.commit()
    conn.close()

def run_scraper(target_url, function, *args):  # Add *args to capture additional arguments
    try:
        function(target_url, *args)  # Pass additional arguments to the scraper function
    except Exception as e:
        print(f"Error running scraper: {e}")

def main():
    djini_url = "https://djinni.co/jobs/?primary_keyword=Python&region=UKR&employment=remote&salary=500&editorial=has_public_salary"
    rabota_start_page = 1
    rabota_end_page = 5

    djini_thread = threading.Thread(target=run_scraper, args=(djini_url, scrape_djini))
    rabota_thread = threading.Thread(target=run_scraper, args=(rabota_start_page, rabota_end_page, scrapy_rabota))

    # Start the threads
    djini_thread.start()
    rabota_thread.start()

    # Wait for threads to finish (optional)
    djini_thread.join()
    rabota_thread.join()


if __name__ == "__main__":
    main()