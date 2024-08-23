import requests
from bs4 import BeautifulSoup
import db
import datetime
import time
import schedule
import pprint
#parser Djini.co

class Djini_parser():

    def __init__(self,url):
        self.url = url

    @property
    def cookies(self):
        cookies = {
            'csrftoken': 'nrSn0Ogay0iBrUKWG55AkxEHmQn8lRBs',
            '_pk_id.1.df23': 'f02d181120e9ad59.1723053799.',
            'intercom-id-cg6zpunb': 'b1201809-20cb-4f25-a1fb-d51e633be902',
            'intercom-device-id-cg6zpunb': 'd3491c25-574d-47ab-9566-2a586c34e4af',
            'sessionid': '.eJwVzEsKwjAURuG93KlW_jx7my04K3UsaR4iSAK1DqRk78bp4eMcFOqn7Nv3HmpM5Oh2nelMvtRC7qBn7MnCTlr4mFmvGpJXE8eMpLw0GixM92FLfk9_LCH1AB4wLoIdlBN8UXZigRPggI7zyz_e_d5a-wEUaiK0:1seWxB:ng_sBS32_vPWqvturXiLF4E_nHB-yLk9yJFINfRudxs',
            '_pk_ses.1.df23': '1',
            'intercom-session-cg6zpunb': '',
        }
        return cookies
    
    @property
    def headers(self):
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            # 'Cookie': 'csrftoken=nrSn0Ogay0iBrUKWG55AkxEHmQn8lRBs; _pk_id.1.df23=f02d181120e9ad59.1723053799.; intercom-id-cg6zpunb=b1201809-20cb-4f25-a1fb-d51e633be902; intercom-device-id-cg6zpunb=d3491c25-574d-47ab-9566-2a586c34e4af; sessionid=.eJwVzEsKwjAURuG93KlW_jx7my04K3UsaR4iSAK1DqRk78bp4eMcFOqn7Nv3HmpM5Oh2nelMvtRC7qBn7MnCTlr4mFmvGpJXE8eMpLw0GixM92FLfk9_LCH1AB4wLoIdlBN8UXZigRPggI7zyz_e_d5a-wEUaiK0:1seWxB:ng_sBS32_vPWqvturXiLF4E_nHB-yLk9yJFINfRudxs; _pk_ses.1.df23=1; intercom-session-cg6zpunb=',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Microsoft Edge";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        return headers
    
    @property
    def params(self):
        params = {
            'primary_keyword': 'Python',
            'region': 'UKR',
            'employment': 'remote',
            'salary': '500',
            'editorial': 'has_public_salary',
        }
        return params

    def parser(self):
        try:
            response = requests.get(
                self.url,
                params=self.params,
                cookies=self.cookies,
                headers=self.headers,
            )
            response.raise_for_status()  # Raise an exception for non-200 status codes

            soup = BeautifulSoup(response.text, 'html.parser')

            allhref = soup.find_all('a', class_='job-item__title-link')
            now = datetime.datetime.now()
            data = []
            for i in allhref:
                job_title = i.get_text(strip=True)
                job_url = i['href']
                data.append({
                    "Url": job_url,
                    "name": job_title,
                })
            
            return data

        except requests.exceptions.RequestException as ex:
            print(f"Error making request: {ex}")
        except Exception as ex:  # Catch other unexpected errors
            print(f"Unexpected error during parsing: {ex}")

        

def scrape_and_store_data():
    url = "https://djinni.co/jobs/?primary_keyword=Python&region=UKR&employment=remote&salary=500&editorial=has_public_salary"
    pars = Djini_parser(url)
    data = pars.parser()  # Get data from the parser

    try:
        bd = db.parser_db()
        bd.create_djini()
        bd.insert_djini(data)
    except Exception as ex:
        print(f"Error connecting to or interacting with database: {ex}")



schedule.every(12).minutes.do(scrape_and_store_data)

while True:
    schedule.run_pending()
    time.sleep(1)