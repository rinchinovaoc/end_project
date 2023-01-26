from bs4 import BeautifulSoup
import requests
from work_psql import save_input_data
from datetime import datetime
from work_psql import conn_exec_query

conn_id = 'postgre_conn'

def parse_source():
    url = 'https://lenta.ru/rss/'
    # url = 'https://www.vedomosti.ru/rss/news'
    # url = 'https://tass.ru/rss/v2.xml'
    page = requests.get(url)
    allNews = []
    news = BeautifulSoup(page.text, "html.parser")
    save_input_data(data=news)
    # print(news)
    allNews = news.findAll('item')
    for new in allNews:
        # print(new)
        # print(new.link.text)
        # print(new.enclosure.url)
        # if not new.pubDate is None
        # new_pubdate = datetime.strptime(, '%a, %d %b %Y %X %z')
        # print(new_pubdate)
        allCategory = []
        allCategory = new.findAll('category')
        for category in allCategory:
            try:
                query = "INSERT INTO public.d_news (source, author, title, pubdate, category) select '"+url+"','"+new.author.text+"', '"+new.title.text+"','"+new.pubdate.text+"','"+new.category.text+"'"
                conn_exec_query(conn_id = 'postgre_conn', query = query)
            except:
                print(Exception,'!!!!!!')
     