import os
import requests
from bs4 import BeautifulSoup
try:
    import feedparser
except:
    os.system('pip3 install feedparser')
    import feedparser
    
from datetime import datetime




class RSS_HABR_PARSER:
    def __init__ (self, python_wiki_rss_url='https://habr.com/ru/rss/all/all/?fl=ru'):
        #self.python_wiki_rss_url = "https://habr.com/ru/rss/feed/posts/all/aeb8fc04f9b286c21bba77f74e9b0d83/?fl=ru"
        self.python_wiki_rss_url = python_wiki_rss_url
        self.session_dict = {}
        self.session = False
        self.custom_header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
        
    def __add_session(self):
        self.session_start = requests.Session()
        self.session_dict[datetime.now()] = self.session_start
        return self.session_start
        
    def get_rss(self, href):
        if self.session == False:
            self.session = self.__add_session()
        dict_for_result_rss_parse = {}
        response = feedparser.parse(href)
        for i, feed in enumerate(response['entries']):
            author_name, body = self.parse_news(feed['link'])
            dict_for_result_rss_parse[i]={'title':feed['title'], 'link':feed['link'], 'published':feed['published'],
                                          'author_name':feed['author_detail']['name'], 'real_author_name':author_name, 'body':body} 
        return dict_for_result_rss_parse

    def parse_news(self, href):
        if self.session == False:
            self.session = self.__add_session()
        response = self.session.get(href, headers=self.custom_header)
        soup = BeautifulSoup(response.content)
        try:
            body = soup.find('body').find(id='post-content-body').text
        except:
            body = ''
        try:
            original_author_name = original_author_name = body.find('a',{'class':'tm-article-presenter__origin-link'}).find('span').text.replace('\n', '').replace('  ', '')
        except:
            original_author_name = ''
        
        return original_author_name, body
    
    def parse_hab(self):
        dict_for_result = self.get_rss(self.python_wiki_rss_url)
        return dict_for_result
        