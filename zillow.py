import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time

class Zillow():
    def __init__(self):
        
        
        self.headers= {
                        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'accept-encoding':'gzip, deflate, sdch, br',
                        'accept-language':'en-GB,en;q=0.8,en-US;q=0.6,ml;q=0.4',
                        'cache-control':'max-age=0',
                        'upgrade-insecure-requests':'1',
                        'user-agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
            }
        self.data=[]

    def create_url(self, zipcode, city, county, method_url):
        
        self.zipcode = zipcode
        self.city = city
        self.county = county

        self.method_url= method_url

        if self.method_url==0:
            url = "https://www.zillow.com/homes/for_sale/{0}/0_singlestory/days_sort".format(self.zipcode)
        if self.method_url==1:
            url = "https://www.zillow.com/homes/for_sale/{0}/0_singlestory/pricea_sort".format(self.zipcode)
        
        self.url = url

        return self.url

    def query_zillow_requests(self):
        res=requests.get(url=self.url, headers=self.headers)

        self.status_code = res.status_code
        self.html = res.text

        return (self.url, self.status_code)
    
    def extract_zillow_info(self):
        if self.status_code==200:
          soup = BeautifulSoup(self.html, 'html.parser')
          contents = soup.find_all('a',class_='list-card-link list-card-link-top-margin list-card-img')
          
          for content in contents:
              self.url_zillow = str(content.attrs.get('href'))

              dicc_info = {'date': datetime.datetime.now() ,'zipcode:':str(self.zipcode), 'city:' : str(self.city), 'county' : str(self.county),  'url': self.url_zillow }
              dicc_info.update(self.extract_zillow_info_detail())
              print(dicc_info)
              self.data.append(dicc_info)

        return self.data

    def extract_zillow_info_detail(self):
        
        req = requests.get(url=self.url_zillow, headers=self.headers)
        soup = BeautifulSoup(req.text, "lxml")

        dicc_zillow = {}

        i = 0
        dicc = {}

        for sub_soup in soup.find_all('div', class_='ds-home-details-chip'):
            dicc[i] = sub_soup.find(
                class_='Text-c11n-8-18-0__aiai24-0 StyledHeading-c11n-8-18-0__ktujwe-0 gcaUyc sc-pscky cYZqfq').get_text()
            i = i+1

        dicc_zillow['price:'] = str(dicc[0])

        i = 0
        dicc = {}

        for sub_soup in soup.find_all('h3', class_='ds-bed-bath-living-area-container'):
            for sub_sub_soup in soup.find_all('span', class_='ds-bed-bath-living-area', limit=3):
                dicc[i] = sub_sub_soup.get_text()
                i = i+1

        for j in range(i):
            dicc_zillow[str(dicc[j]).split(' ')[1]] = str(dicc[j]).split(' ')[0]

        dicc_tag = {}
        dicc_value = {}

        for sub_soup in soup.find_all('ul', class_='ds-home-fact-list'):
            i = 0
            for sub_sub_soup in soup.find_all('span', class_='Text-c11n-8-18-0__aiai24-0 sc-pTWqp kdrGgn', limit=10):
                dicc_tag[i] = sub_sub_soup.get_text()
                i = i+1
            i = 0
            for sub_sub_soup in soup.find_all('span', class_='Text-c11n-8-18-0__aiai24-0 foiYRz', limit=10):
                dicc_value[i] = sub_sub_soup.get_text()
                i = i+1
        for j in range(i):
            try:
                dicc_zillow[str(dicc_tag[j])] = str(dicc_value[j])
            except:
                continue

        return dicc_zillow

    def Create_file_zillow_excel(self):
        df=pd.DataFrame(self.data)
        df = df.drop_duplicates(subset=['url'])
        df.to_excel('info.xlsx', sheet_name='master', index=False)
        
        return df
        
    def sleep_scrapper_zillow(self):
        if self.status_code==200:
            print(self.status_code)
            time.sleep(10)
        else:
            print(self.status_code)
            time.sleep(200)
        return self.status_code

io='data.xlsx'

df = pd.read_excel(io=io, sheet_name='master')
mizillow = Zillow()

for i in range(df.shape[0]):
    if df.loc[i, 'state_of_query'] == 1:
        
        zipcode = df.loc[i, 'zipcode']
        city = df.loc[i, 'city']
        county = df.loc[i, 'county']

        try:
            mizillow.create_url(zipcode=zipcode,city=city,county=county, method_url=0)
            mizillow.query_zillow_requests()
            mizillow.extract_zillow_info()
            mizillow.sleep_scrapper_zillow()

            mizillow.create_url(zipcode=zipcode,city=city,county=county, method_url=1)
            mizillow.query_zillow_requests()
            mizillow.extract_zillow_info()
            mizillow.sleep_scrapper_zillow()
        except:
            continue

mizillow.Create_file_zillow_excel()

df.loc[i, 'last_date_of_query'] = datetime.datetime.now()

df.to_excel(io, sheet_name='master', index=False)
