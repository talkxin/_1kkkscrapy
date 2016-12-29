# -*- coding: utf-8 -*-
from scrapy.spiders import BaseSpider
from scrapy.selector import Selector
from scrapy import Request
from _1kkk.items import KkkItem
from _1kkk.items import Chapter
from _1kkk.items import Page
from _1kkk.pipelines import MangaDao
from _1kkk.pipelines import Manga
from selenium import webdriver
import urllib.request#python3
import re
import os
import time

class ManSpider(BaseSpider):
    name="manhua"
    start_urls=[]
    dao=MangaDao()
    """
        获取数据库中所有需要爬取的漫画
    """
    driver = webdriver.PhantomJS(executable_path='./bin/phantomjs')
    for i in dao.getMangas():
        #判断该漫画是否在连载中
        if i.state==None or int(i.state)==1:
            start_urls.append(i.pageurl)
    #所有的漫画
    items={}
    #所有的章节
    chids={}
    def parse(self, response):
        re1='.*?'	# Non-greedy match on filler
        re2='\\d+'	# Uninteresting: int
        re3='.*?'	# Non-greedy match on filler
        re4='(\\d+)'	# Integer Number 1
        rg = re.compile(re1+re2+re3+re4,re.IGNORECASE|re.DOTALL)
        m = rg.search(response.url)
        stats=response.xpath("//ul[@class='sy_k22 z ma3 mt5']/li")
        item=KkkItem()
        item['id']=m.group(1)
        item['url']=response.url
        item['name']=response.xpath("//div[@class='sy_k21']/h1/text()").extract()[0]
        item['state']=stats[1].xpath("font/text()").extract()[0]
        """
            暂时发现漫画类型和作者有可能为空
        """
        if stats[4]!=None and len(stats[4].xpath("a/text()").extract())!=0:
            item['type']=stats[4].xpath("a/text()").extract()[0]
        else:
            item['type']="null"
        if stats[2]!=None and len(stats[2].xpath("a/text()").extract())!=0:
            item['author']=stats[2].xpath("a/text()").extract()[0]
        else:
            item['author']="null"
        item['chapter']=[]
        item['time']=stats[6].xpath("font/text()").extract()[0]
#        print("into start: name:%s,state:%s,author:%s,time:%s"%(name,state,author,time))
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
            re1='.*?'+'\\d+'+'.*?'+'\\d+'+'.*?'+'(\\d+)'
            rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
            m = rg.search(url)
            ci.id=m.group(1)
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
                re1='.*?'+'((?:[a-z][a-z]*[0-9]+[a-z0-9]*))'+'.*?'+'(\\d+)'+'.*?'+'(\\d+)'
                rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
                m = rg.search(furl)
                identifies=str(m.group(1))
                id=str(m.group(2))
                size=str(m.group(3))
                purl="http://www.1kkk.com/"+identifies+"-"+id+"/imagefun.ashx?cid="+id+"&page="+size+"&key=&maxcount=10"
                yield Request(purl,meta={'id': response.meta['id'],'chid':ci.id,'len':int(len)-1,'pagesize':size,'furl':furl}, callback=self.parse_each_page)
            else:
                furl=response.url
                re1='.*?'+'((?:[a-z][a-z]*[0-9]+[a-z0-9]*))'+'.*?'+'(\\d+)'
                rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
                m = rg.search(response.url)
                identifies=str(m.group(1))
                id=str(m.group(2))
                purl="http://www.1kkk.com/"+identifies+"-"+id+"/imagefun.ashx?cid="+id+"&page=1&key=&maxcount=10"
                yield Request(purl,meta={'id': response.meta['id'],'chid':ci.id,'len':int(len)-1,'pagesize':1,'furl':furl}, callback=self.parse_each_page)

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
        jsurl=response.xpath("//text()").extract()
        filepath="./tmp/image/%s/%s/"%(manga.id,ci.id)
        if os.path.exists(filepath) != True:
            os.makedirs(filepath)
        if len(ci.page)!=length:
            page.id=pagesize
            page.imageurl=self.getImgUrl(furl,response.url,0,'%s/%s.jpg'%(filepath,page.id))
            ci.page.append(page)
        else:
            page.id=pagesize
            page.imageurl=self.getImgUrl(furl,response.url,0,'%s/%s.jpg'%(filepath,page.id))
            ci.page.append(page)
            item['item']['chapter'].append(ci)
            if item['hualength']==len(item['item']['chapter']):
                yield item['item']

    def getImgUrl(self,furl,jsurl,max,path):
        size=max
        try:
            if size<3:
                size=size+1
                self.driver.get(jsurl)
                js="""
                    var i;
                    i=document.body.innerText;
                    i=eval(i);
                    var p = document.createElement("div");
                    p.setAttribute("id","__imgurl");
                    p.innerHTML=i[0];
                    document.body.insertBefore(p, document.body.firstChild);
                    """
                self.driver.execute_script(js)
                imageurl=self.driver.find_element_by_id('__imgurl').text
                urllib.request.urlretrieve(imageurl, path)
                return path
            else:
                return ""
        except Exception as e:
                self.driver.get(furl)
                time.sleep(3)
                return self.getImgUrl(furl,jsurl,size)

