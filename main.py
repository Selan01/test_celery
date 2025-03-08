import requests
from bs4 import BeautifulSoup
import xmltodict
from celery import Celery

app = Celery('parsing_task', broker='redis://localhost:6379/0')

app.conf.update(task_always_eager=True)

# Класс для задачи парсинга веб-страницы
class ParseWebpageTask(app.Task):
    def run(self, url):
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code != 200:
            print(f'Ошибка при получении страницы: {response.status_code}')
            return []

        soup = BeautifulSoup(response.text, 'lxml')
        div_a = soup.find_all('div', {'class': 'registry-entry__header-mid__number'})
        links = [a['href'] for div in div_a for a in div.find_all('a', href=True)]
        links = links[:2]
        return links


class FetchXmlTask(app.Task):
    def run(self, url):
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        return response.text


class ParseSingleXmlTask(app.Task):
    def run(self, xml_content):
        data_dict = xmltodict.parse(xml_content)
        first_key = list(data_dict.keys())[0]
        try:
            publish = data_dict[first_key]['commonInfo']['publishDTInEIS'] if \
                data_dict[first_key]['commonInfo']['publishDTInEIS'] != 0 else None
        except:
            publish = None
        print(f'publishDTInEIS: {publish}')
        return publish


class ParseXmlForLinksTask(app.Task):
    def run(self, links, xml_url='https://zakupki.gov.ru/epz/order/notice/printForm/viewXml.html?regNumber='):
        xml_urls = [xml_url + link.split('=')[1] for link in links]

        xml_contents = [FetchXmlTask().run(url) for url in xml_urls]
        for content in xml_contents:
            ParseSingleXmlTask().run(content)


class ParseMultiplePagesTask(app.Task):
    def run(self, urls):
        links_lists = [ParseWebpageTask().run(url) for url in urls]
        for links in links_lists:
            ParseXmlForLinksTask().run(links)


# Пример выполнения задач
if __name__ == '__main__':
    urls = [
        'https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber=1',
        'https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber=2'
    ]

    # Запуск задачи для обработки нескольких страниц
    ParseMultiplePagesTask().run(urls)
