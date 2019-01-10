#!/usr/bin/env python
# coding: utf-8

# In[3]:


import requests
import json
import pymongo
from bs4 import BeautifulSoup
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
# 패키지 임포트는 동일


# In[4]:


base = declarative_base()
# 얘는 글로벌 영역으로ㅂ 빼줘야만 상속을 받아서 클래스를 만들 수 있다
class NaverKeyword(base):
    __tablename__ = "naver"

    id = Column(Integer, primary_key=True)
    rank = Column(Integer, nullable=False)
    keyword = Column(String(50), nullable=False)
    rdate = Column(TIMESTAMP, nullable=False)

    def __init__(self, rank, keyword):
        self.rank = rank
        self.keyword = keyword

    def __repr__(self):
        return "<NaverKeyword {}, {}>".format(self.rank, self.keyword)  


# In[5]:


class NaverKeywords:
    
    def __init__(self, ip, base):
        self.mysql_client = create_engine("mysql://root:6137tnwjd@{}/world?charset=utf8".format(ip)) # ip와 베이스는 꼭 필요
        self.mongo_client = pymongo.MongoClient('mongodb://{}:27017'.format(ip))
        # ip가 동일
        self.datas = None
        # 크롤링하고 난 데이터를 저장할 변수를 만들어주는게 datas
        self.base = base
        
        
    def crawling(self):
        response = requests.get("https://www.naver.com/")
        dom = BeautifulSoup(response.content, "html.parser")
        keywords = dom.select(".ah_roll_area > .ah_l > .ah_item")
        datas = []
        for keyword in keywords:
            rank = keyword.select_one(".ah_r").text
            keyword = keyword.select_one(".ah_k").text
            datas.append((rank, keyword))
        self.datas = datas # 여기만 다름. 크롤링 후에 오브젝트 변수에 저장하는것
    
    
    def mysql_save(self):
        
        # make table
        self.base.metadata.create_all(self.mysql_client) # 앞에 self 붙인것만 다르고 모두 동일.
        
        # parsing keywords
        keywords = [NaverKeyword(rank, keyword) for rank, keyword in self.datas]

        # make session
        maker = sessionmaker(bind=self.mysql_client)
        session = maker()

        # save datas
        session.add_all(keywords)
        session.commit()

        # close session
        session.close()
        
    def mongo_save(self):
        
        # parsing querys
        keyowrds = [{"rank":rank, "keyword":keyword} for rank, keyword in self.datas] # self.data 부분만 다르다.
        
        # insert keyowrds
        self.mongo_client.crawling.naver_keywords.insert(keyowrds) # self. 이런것만 다름.
        
    def send_slack(self, msg, channel="#dss", username="provision_bot" ): # 클래스 안에서 작동되기때문에 반드시 셀프가 있어야 한다.
        webhook_URL = "https://hooks.slack.com/services/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"
        payload = {
            "channel": channel,
            "username": username,
            "icon_emoji": ":provision:",
            "text": msg,
        }
        response = requests.post(
            webhook_URL,
            data = json.dumps(payload),
        )
    
    def run(self):
        self.crawling()
        self.mysql_save()
        self.mongo_save()
        self.send_slack("naver crawling done!")


# In[6]:


nk = NaverKeywords("13.125.47.66", base)
nk.run()

