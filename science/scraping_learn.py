# coding: utf-8
import re
import random
from urllib import urlopen

# if has Chinese, apply decode()
from urlparse import urljoin

import requests
from bs4 import BeautifulSoup


def scraping_baike():
    base_url = "https://baike.baidu.com"
    his = ["/item/%E7%BD%91%E7%BB%9C%E7%88%AC%E8%99%AB/5162711"]

    for i in range(20):
        url = base_url + his[-1]

        html = urlopen(url).read().decode('utf-8')
        soup = BeautifulSoup(html, features='lxml')
        print(i, soup.find('h1').get_text(), '    url: ', his[-1])

        # find valid urls
        sub_urls = soup.find_all("a", {"target": "_blank", "href": re.compile("/item/(%.{2})+$")})

        if len(sub_urls) != 0:
            his.append(random.sample(sub_urls, 1)[0]['href'])
        else:
            # no valid sub link found
            his.pop()


def open_browser():
    import webbrowser
    param = {"wd": "莫烦Python"}  # 搜索的信息
    r = requests.get('http://www.baidu.com/s', params=param)
    print(r.url)
    webbrowser.open(r.url)


def post_reqest():
    data = {'firstname': '莫烦', 'lastname': '周'}
    r = requests.post('http://pythonscraping.com/files/processing.php', data=data)
    print(r.text)




def _multiproc():
    import multiprocessing as mp
    import time
    from urllib import urlopen
    from bs4 import BeautifulSoup
    import re

    # base_url = "http://127.0.0.1:4000/"
    base_url = 'https://morvanzhou.github.io/'
    if base_url != "https://morvanzhou.github.io/":
        restricted_crawl = True
    else:
        restricted_crawl = False
    def crawl(url):
        response = urlopen(url)
        # time.sleep(0.1)             # slightly delay for downloading
        return response.read().decode('utf-8')

    def parse(html):
        soup = BeautifulSoup(html, 'lxml')
        urls = soup.find_all('a', {"href": re.compile('^/.+?/$')})
        title = soup.find('h1').get_text().strip()
        page_urls = set([urljoin(base_url, url['href']) for url in urls])  # 去重
        url = soup.find('meta', {'property': "og:url"})['content']
        return title, page_urls, url

    unseen = set([base_url, ])
    seen = set()

    count, t1 = 1, time.time()
    pool = mp.Pool(4)
    while len(unseen) != 0:  # still get some url to visit
        if restricted_crawl and len(seen) > 20:
            break

        print('\nDistributed Crawling...')
        # htmls = [crawl(url) for url in unseen]
        crawl_jobs = [pool.apply_async(crawl, args=(url,)) for url in unseen]
        htmls = [j.get() for j in crawl_jobs]

        print('\nDistributed Parsing...')
        # results = [parse(html) for html in htmls]
        parse_jobs = [pool.apply_async(parse, args=(html,)) for html in htmls]
        results = [j.get() for j in parse_jobs]

        print('\nAnalysing...')
        seen.update(unseen)  # seen the crawled
        unseen.clear()  # nothing unseen

        for title, page_urls, url in results:
            print(count, title, url)
            count += 1
            unseen.update(page_urls - seen)  # get new url to crawl
    print('Total time: %.1f s' % (time.time() - t1,))  # 53 s


if __name__ == "__main__":
    _multiproc()
