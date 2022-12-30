from bs4 import BeautifulSoup
import requests
from work_psql import save_input_data

def parse_source():
    url = 'https://tass.ru/rss/v2.xml'
    page = requests.get(url)
    print(page.status_code)
    allNews = []
    news = BeautifulSoup(page.text, "html.parser")
    print(news)
    save_input_data(data=news)

    allNews = news.findAll('item')
    for new in allNews:
        #if data.find('span', class_='time2 time3') is not None:
        print(new.title.text)
        print(new.link)
        
        allCategory = []
        allCategory = new.findAll('category')
        for category in allCategory:
            print(category.text)

        #filteredNews.append(data.category.text)
   # for data in filteredNews:
    #    print(data)