import requests
from bs4 import BeautifulSoup
import pprint
#psarser jobs.dou.ua
from bd import create_dou, insert_dou,connect_to_db
import schedule
import time
class job_dou_ua():

    def __init__(self,url):
        self.url=url

    @property
    def cookies(self):
        cookies = {
            'csrftoken': '4zgkElk0o5fqrhtyv0C45PHyz4fNlsVX',
            '_gcl_au': '1.1.299299920.1723753311',
            '_ga_N62L6SV4PV': 'GS1.1.1723753311.1.0.1723753311.60.0.0',
            '_ga': 'GA1.1.345830554.1723753312',
            '_fbp': 'fb.1.1723753311775.170137586454487518',
        }
        return cookies
    @property
    def headers(self):
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
            'cache-control': 'max-age=0',
            # 'cookie': 'csrftoken=4zgkElk0o5fqrhtyv0C45PHyz4fNlsVX; _gcl_au=1.1.299299920.1723753311; _ga_N62L6SV4PV=GS1.1.1723753311.1.0.1723753311.60.0.0; _ga=GA1.1.345830554.1723753312; _fbp=fb.1.1723753311775.170137586454487518',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Microsoft Edge";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0',
        }
        return headers
    @property
    def params(self):
        params = {
            'city': 'remote',
        }
        return params
    
    def parser_dou(self):

        try:
            response = requests.get(
                self.url,
                params=self.params,
                cookies=self.cookies,
                headers=self.headers,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            href_all = soup.find_all('a',class_="vt")
            data = []
            for i in href_all:
                job_title = i.get_text(strip=True)
                job_url = i["href"]
                data.append({
                    "Url": job_url,
                    "name": job_title
                })
            return data
        except Exception as ex:
            print(ex)
def scrape_and_store_data():
    url = "https://jobs.dou.ua/first-job/?city=remote"
    pars = job_dou_ua(url)
    data = pars.parser_dou()

    conn = connect_to_db()
    create_dou(conn)
    insert_dou(conn, data)
    conn.commit
    conn.close()

schedule.every(1).minutes.do(scrape_and_store_data)

while True:
    schedule.run_pending()
    time.sleep(1)