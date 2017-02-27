# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector
from scrapy import Request
from _1kkk.items import KkkItem
from _1kkk.items import Chapter
from _1kkk.items import Page
from _1kkk.pipelines import MangaDao
from _1kkk.pipelines import Manga
import copy
import execjs
import requests
import urllib.request#python3
import re
import os
import os.path
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

class ManSpider(scrapy.Spider):

    global phantomjspath
    executor = ThreadPoolExecutor(max_workers=5)
    #创建锁
    mutex = threading.Lock()
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
    """
        获取数据库中所有需要爬取的漫画
    """
    for i in dao.getMangas():
        # #判断该漫画是否在连载中
        # if i.state==None or int(i.state)==1:
        start_urls.append(i.pageurl)
    #所有的漫画
    items={}
    #所有的章节
    chids={}
    def parse(self, response):
        operator = {"http://www.1kkk.com/":self._1kkk_parse,"http://www.cartoonmad.com/":self._cartoonmad_parse}
        url=response.urljoin("/")
        return operator.get(url)(response)

    def _cartoonmad_parse(self,response):
        item=KkkItem()
        stats=response.xpath("//table[@style='font-size:11pt;']/tr/td")
        item['id']=response.url[response.url.rfind("/")+1:response.url.rfind(".")]
        item['url']=response.url
        name=response.xpath("//title/text()").extract()[0]
        item['name']=name[:name.find("-")-1]
        item['state']=1
        item['time']="1970-01-01"
        if stats[6]!=None:
            str=stats[6].xpath('./text()').extract()[0]
            item['author']=str[str.find("：")+1:].strip()
        else:
            item['author']="null"
        if stats[4]!=None and len(stats[4].xpath("a/text()").extract())!=0:
            item['type']=stats[4].xpath("a/text()").extract()[0]
        else:
            item['type']="null"
        huasz=response.xpath("//table[@width='688']/tr/td/a")
        self.items[item['id']]={'item':item,'hualength':len(huasz)}
        for hua in huasz:
            ci=Chapter()
            ci.chid=hua.xpath("text()").extract()[0].replace(" ","")
            href=hua.xpath("@href").extract()[0]
            url =response.urljoin(href)
            ci.id=href[href.rfind("/")+1:href.rfind(".")]
            """
                过滤数据库中所有已经下载过的漫画
            """
            if self.dao.getMangaPageByKkkid(ci.id)==None:
                yield Request(url,meta={'id': item['id'],'chid':ci,'href':href}, callback=self._cartoonmad_parse_each_chapter)

    def _cartoonmad_parse_each_chapter(self,response):
        ci=response.meta['chid']
        ci.page=[]
        self.chids[ci.id]=ci
        length=len(response.xpath("//select[@name='jump']/option/text()").extract())
        url=response.xpath("//img[contains(@src,'cartoonmad.com')]/@src").extract()[0]
        url=url[:url.rfind("/")+1]
        # re1='.*?c(?:[a-z][a-z0-9_]*).*?(?:[a-z][a-z0-9_]*).*?(?:[a-z][a-z0-9_]*).*?(?:[a-z][a-z0-9_]*).*?((?:[a-z][a-z0-9_]*))'
        # rg = re.compile(re1,re.IGNORECASE|re.DOTALL)
        # m = rg.search(url)
        queue=[]
        #上锁等待
        if len(self.db.getNotBackupManga())>10:
            self.mutex.acquire()
            while len(self.db.getNotBackupManga())!=0:
                time.sleep(10)
                continue
            self.mutex.release()
        for i in range(1,length):
            purl="%s/%0*d.jpg"%(url,3,i)
            queue.append(self.executor.submit(self.parse_each_page,response.meta['id'],ci,length,i,purl,purl,2))
        for o in as_completed(queue):
            if o.result()>=int(length-1):
                self.items[response.meta['id']]['item']['chapter']=self.chids[ci.id]
                yield self.items[response.meta['id']]['item']


    def _1kkk_parse(self,response):
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
                str=""
                for at in stats[2].xpath("a/text()").extract():
                    str+=at+" "
                item['author']=str[:-1]
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
                str=""
                for at in stats[1].xpath("a/text()").extract():
                    str+=at+" "
                item['author']=str[:-1]
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
            if self.verify(ci.chid[1:-1]):
               ci.chid="%s%0*d%s"%(ci.chid[:1],3,int(ci.chid[1:-1]),ci.chid[-1:])
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
                yield Request(url,meta={'id': item['id'],'chid':ci,'href':href}, callback=self.parse_each_chapter)

    def parse_each_chapter(self, response):
        ci=response.meta['chid']
        ci.page=[]
        self.chids[ci.id]=ci
        href=response.meta['href']
        len=response.xpath("//font[@class='zf40']/span[last()]/text()").extract()[0]
        id=href.split('-')[-1][:-1]
        identifies=href[1:href.find(id)-1]
        queue=[]
        #上锁等待
        if len(self.db.getNotBackupManga())>10:
            self.mutex.acquire()
            while len(self.db.getNotBackupManga())!=0:
                time.sleep(10)
                continue
            self.mutex.release()
        for i in range(1,int(len)+1):
            if i!=1:
                furl=str(response.url)[:-1]+"-p"+str(i)
                purl="http://www.1kkk.com/"+identifies+"-"+id+"/imagefun.ashx?cid="+id+"&page="+str(i)+"&key=&maxcount=10"
                queue.append(self.executor.submit(self.parse_each_page,response.meta['id'],ci,int(len)-1,i,furl,purl,1))
            else:
                furl=response.url
                purl="http://www.1kkk.com/"+identifies+"-"+id+"/imagefun.ashx?cid="+id+"&page=1&key=&maxcount=10"
                queue.append(self.executor.submit(self.parse_each_page,response.meta['id'],ci,int(len)-1,1,furl,purl,1))
        for o in as_completed(queue):
            if o.result()>=int(len):
                self.items[response.meta['id']]['item']['chapter']=self.chids[ci.id]
                yield self.items[response.meta['id']]['item']

    """
        获取所有页面的js数据，并开始对js数据进行处理
    """
    def parse_each_page(self,id,ci,length,pagesize,furl,purl,type):
        item=self.items[id]
        manga=self.dao.getMangaByUrl(item['item']['url'])
        rp=Page()
        filepath="./tmp/image/%s/%s/"%(manga.id,ci.id)
        try:
            if not os.path.isdir(filepath):
                os.makedirs(filepath)
        except Exception as e:
            logging.warning(str(e))
        operator = {1:self._kkk_getImgUrl,2:self._cartoonmad_getImgUrl}
        rp.id=pagesize
        rp.imageurl=operator.get(type)(furl,purl,'%s/%s.jpg'%(filepath,rp.id))
        self.chids[ci.id].page.append(rp)
        return len(self.chids[ci.id].page)

    '''
        获取极速漫画图片
    '''
    def _kkk_getImgUrl(self,furl,jsurl,path):
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
            logging.warning(str(e))
            time.sleep(3)
            self.getImgUrl(furl,jsurl,path)

    '''
        获取动漫狂图片
    '''
    def _cartoonmad_getImgUrl(self,furl,jsurl,path):
        if os.path.exists(path):
            return path
        time.sleep(5)
        r = requests.get(jsurl)
        with open(path, 'wb') as f:
            f.write(r.content)
        return path

    def verify(self,num):
        try:
            return (False, True)[round(float(num)) == float(num)]
        except Exception as e:
            return False
    '''
        检查文件夹大小
    '''
    def getdirsize(dir):
        size = 0
        for root, dirs, files in os.walk(dir):
            size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
        return size
