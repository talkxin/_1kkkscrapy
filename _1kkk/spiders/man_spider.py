# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector
from scrapy import Request
from _1kkk.items import KkkItem
from _1kkk.items import Chapter
from _1kkk.items import Page
from _1kkk.pipelines import MangaDao
from _1kkk.pipelines import Manga
#from selenium import webdriver
import copy
import execjs
import requests
import urllib.request#python3
import re
import os
import os.path
import time
#import platform

class ManSpider(scrapy.Spider):
    
    global phantomjspath
#    sysstr = platform.system()
#    if sysstr == "Linux":
#        phantomjspath='./bin/phantomjs_linux'
#    else:
#        phantomjspath='./bin/phantomjs_mac'
    name="manhua"
    start_urls=[]
    dao=MangaDao()
    headers = {'Pragma': 'no-cache',
                'DNT': '1',
                'Accept-Encoding': 'gzip, deflate, sdch',
                'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36',
                'Accept': 'image/webp,image/*,*/*;q=0.8',
                'Referer': 'http://www.1kkk.com/ch1-116859/',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache'
                }
    #    cap = webdriver.DesiredCapabilities.PHANTOMJS
    #    cap["phantomjs.page.settings.resourceTimeout"] = 5000
    #    cap["phantomjs.page.settings.userAgent"] = "faking it"
#    cap = webdriver.DesiredCapabilities.PHANTOMJS
#    cap["phantomjs.page.settings.loadImages"] = False
#    cap["phantomjs.page.settings.disk-cache"] = True
#    driver = webdriver.PhantomJS(executable_path=phantomjspath,desired_capabilities=cap)
#    driver.set_page_load_timeout(120)
    """
        获取数据库中所有需要爬取的漫画
    """
    for i in dao.getMangas():
        #判断该漫画是否在连载中
        if i.state==None or int(i.state)==1:
            start_urls.append(i.pageurl)
    #所有的漫画
    items={}
    #所有的章节
    chids={}
    def parse(self, response):
        re1='.*?'   # Non-greedy match on filler
        re2='\\d+'  # Uninteresting: int
        re3='.*?'   # Non-greedy match on filler
        re4='(\\d+)'    # Integer Number 1
        rg = re.compile(re1+re2+re3+re4,re.IGNORECASE|re.DOTALL)
        m = rg.search(response.url)
        stats=response.xpath("//ul[@class='sy_k22 z ma3 mt5']/li")
        item=KkkItem()
        item['id']=m.group(1)
        item['url']=response.url
        item['name']=response.xpath("//div[@class='sy_k21']/h1/text()").extract()[0]
        if  stats[0]==None or len(stats[0].xpath("font/text()").extract())==0:
            if stats[1]!=None and len(stats[1].xpath("font/text()").extract())!=0:
                item['state']=stats[1].xpath("font/text()").extract()[0]
            else:
                item['state']="已完结"
            if stats[4]!=None and len(stats[4].xpath("a/text()").extract())!=0:
                item['type']=stats[4].xpath("a/text()").extract()[0]
            else:
                item['type']="null"
            if stats[2]!=None and len(stats[2].xpath("a/text()").extract())!=0:
                item['author']=stats[2].xpath("a/text()").extract()[0]
            else:
                item['author']="null"
            item['time']=stats[6].xpath("font/text()").extract()[0]
        else:
            if stats[0]!=None and len(stats[1].xpath("font/text()").extract())!=0:
                item['state']=stats[0].xpath("font/text()").extract()[0]
            else:
                item['state']="已完结"
            if stats[3]!=None and len(stats[3].xpath("a/text()").extract())!=0:
                item['type']=stats[3].xpath("a/text()").extract()[0]
            else:
                item['type']="null"
            if stats[1]!=None and len(stats[1].xpath("a/text()").extract())!=0:
                item['author']=stats[1].xpath("a/text()").extract()[0]
            else:
                item['author']="null"
            item['time']=stats[5].xpath("font/text()").extract()[0]
        item['chapter']=[]
        huas=response.xpath("//ul[@class='sy_nr1 cplist_ullg']/li/a")
        huasz=[]
        """
            过滤所有外传类漫画
        """
        for c in huas:
            href=c.xpath("@href").extract()[0];
            if href.find("-")!=-1:
                huasz.append(c)
        self.items[item['id']]={'item':item,'hualength':len(huasz)}
        for hua in huasz:
            ci=Chapter()
            ci.chid=hua.xpath("text()").extract()[0]
            href=hua.xpath("@href").extract()[0]
            url =response.urljoin(href)
            re1='.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'((?:[a-z][a-z0-9_]*))'+'.*?'+'(\\d+)'
            rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
            m = rg.search(url)
            ci.id=m.group(2)
            """
                过滤数据库中所有已经下载过的漫画
            """
            if self.dao.getMangaPageByKkkid(ci.id)==None:
                yield Request(url,meta={'id': item['id'],'chid':ci}, callback=self.parse_each_chapter)


    def parse_each_chapter(self, response):
        ci=response.meta['chid']
        ci.page=[]
        self.chids[ci.id]=ci
        len=response.xpath("//font[@class='zf40']/span[last()]/text()").extract()[0]
        for i in range(1,int(len)+1):
            if i!=1:
                furl=str(response.url)[:-1]+"-p"+str(i)
                re1='.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'((?:[a-z][a-z0-9_]*))'+'.*?'+'(\\d+)'+'.*?'+'(\\d+)'
                rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
                m = rg.search(furl)
                identifies=str(m.group(1))
                id=str(m.group(2))
                size=str(m.group(3))
                purl="http://www.1kkk.com/"+identifies+"-"+id+"/imagefun.ashx?cid="+id+"&page="+size+"&key=&maxcount=10"
                yield Request(furl,meta={'id': response.meta['id'],'chid':ci.id,'len':int(len)-1,'pagesize':size,'furl':purl}, callback=self.parse_each_page)
            else:
                furl=response.url
                re1='.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'(?:[a-z][a-z0-9_]*)'+'.*?'+'((?:[a-z][a-z0-9_]*))'+'.*?'+'(\\d+)'+'.*?'+'(\\d+)'
                rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
                m = rg.search(furl)
                identifies=str(m.group(1))
                id=str(m.group(2))
                purl="http://www.1kkk.com/"+identifies+"-"+id+"/imagefun.ashx?cid="+id+"&page=1&key=&maxcount=10"
                yield Request(furl,meta={'id': response.meta['id'],'chid':ci.id,'len':int(len)-1,'pagesize':1,'furl':purl}, callback=self.parse_each_page,errback=self.parse_each_page)

    """
        获取所有页面的js数据，并开始对js数据进行处理
        若出现超时或报错，则对父页面进行重新拉取刷新，并暂停3秒钟再次尝试拉取
        每个页面尝试3次，超过3次的均返回为空
    """
    def parse_each_page(self,response):
        item=self.items[response.meta['id']]
        manga=self.dao.getMangaByUrl(item['item']['url'])
        ci=self.chids[response.meta['chid']]
        length=response.meta['len']
        pagesize=response.meta['pagesize']
        furl=response.meta['furl']
        page=Page()
        filepath="./tmp/image/%s/%s/"%(manga.id,ci.id)
        if os.path.exists(filepath) != True:
            os.makedirs(filepath)
        if len(ci.page)<length:
            page.id=pagesize
            page.imageurl=self.getImgUrl(response.url,furl,'%s/%s.jpg'%(filepath,page.id))
            ci.page.append(page)
        else:
            page.id=pagesize
            page.imageurl=self.getImgUrl(response.url,furl,'%s/%s.jpg'%(filepath,page.id))
            ci.page.append(page)
            item['item']['chapter']=[ci]
#            item['item']['chapter'].append(ci)
#            if item['hualength']==len(item['item']['chapter']):
            yield item['item']

    def getImgUrl(self,furl,jsurl,path):
        try:
            if os.path.exists(path):
                return path
            requests.get(furl)
            myheaders = copy.copy(self.headers)
            myheaders['Referer'] = furl
            r1 = requests.get(jsurl, headers=myheaders)
            func = execjs.eval(r1.text[4:])
            func2 = execjs.compile(func).call("dm5imagefun")[0]
            r = requests.get(func2, headers=myheaders)
            with open(path, 'wb') as f:
                f.write(r.content)
            return path
        except Exception as e:
            print(e)
            time.sleep(3)
            self.getImgUrl(furl,jsurl,path)
#    def getImgUrl(self,furl,jsurl,max,path):
#        if os.path.exists(path):
#            return path
#        try:
#            if max<5:
#                max=max+1
#                self.driver.get(jsurl)
#                js="""
#                    var i;
#                    i=document.body.innerText;
#                    i=eval(i);
#                    var p = document.createElement("div");
#                    p.setAttribute("id","__imgurl");
#                    p.innerHTML=i[0];
#                    document.body.insertBefore(p, document.body.firstChild);
#                    """
#                self.driver.execute_script(js)
#                imageurl=self.driver.find_element_by_id('__imgurl').text
#                urllib.request.urlretrieve(imageurl, path)
##                self.driver.quit()
#                return path
#            else:
#                return ""
#        except Exception as e:
##                print("download error %s"%e)
#                self.driver.quit()
#                self.driver = None
#                self.driver = webdriver.PhantomJS(executable_path=phantomjspath,desired_capabilities=self.cap)
#                self.driver.set_page_load_timeout(120)
#                self.driver.get(furl)
#                time.sleep(3)
#                return self.getImgUrl(furl,jsurl,max,path)
