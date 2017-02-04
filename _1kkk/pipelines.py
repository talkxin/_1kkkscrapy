10# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import os,os.path
import zipfile
#import requests
import threading
import queue
import time
import sqlite3
import struct
import pickle
import shutil
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#from libs import ebooklib
#from _1kkk.libs.ebooklib.ebooklib import epub
from _1kkk.libs.kcc.kcc.comic2ebook import createKVBook
from _1kkk.libs.baidupcsapi.baidupcsapi import PCS
from _1kkk.items import KkkItem
import json

class KkkPipeline(object):
    
    def open_spider(self, spider):
        #初始化队列文件
        self.man=downloadImage()
        self.man.start()
        self.db=MangaDao()
    
    def close_spider(self, spider):
        self.man.put("out")

    """
        每个漫画创建一个下载线程进行下载
        下载完成后生成epub格式的书籍进行备份
        同时将epub格式的书籍与百度云盘进行同步
        并且根据user表中定义的用户，将数据推送至该用户的kindle
    """
    def process_item(self, item, spider):
        """
            v1.0为每一个漫画一个线程，则会遇到多个线程同时下载的情况。
                在小服务器的情况下会导致内存溢出的bug。
            v1.1改造为线程池根据服务器情况动态调整线程池的最大处理上线，来防止生成epub时导致内存溢出。
        """
        url=item['url']
        manga=self.db.getMangaByUrl(url)
        manga.kkkid=item['id']
        manga.name=item['name']
        manga.ci=item['chapter']
        if manga.state!=None and manga.state!=0 and item['state']=="连载中":
            manga.state=1
        else:
            manga.state=0
        manga.type=item['type']
        manga.time=item['time']
        manga.author=item['author']
        self.db.updateManga(manga)
        self.man.put(manga)
        return item


class downloadImage(threading.Thread):
    def __init__(self):
        self.db=MangaDao()
        self.queue=queue.Queue(0)
        #默认读取首位用户
        self.user=self.db.getUserbyID(1)
        
        self.smtp = smtplib.SMTP()
        self.smtp.connect(self.user.sendMail_smtp)
        
        #登陆smtp
        if self.user.sendMail_username!=None and self.user.sendMail_username!="" and self.user.sendMail_password!=None and self.user.sendMail_password!="":
            self.smtp.login(self.user.sendMail_username, self.user.sendMail_password)

        self.pcs = PCS(self.user.baiduname,self.user.baidupass)
        while json.loads(self.pcs.quota().content.decode())['errno']==-6:
            time.sleep(3)
            self.pcs = PCS(self.user.baiduname,self.user.baidupass)
        super().__init__()
    
    def put(self,items):
        self.queue.put(items)
    
    def run(self):
        while True:
            items=self.queue.get()
            if(items!="out"):
                self.initManga(items)
            else:
                break
    

    def initManga(self,items):
        manga=items
        for ci in manga.ci:
            mPage=MangaPage()
            mPage.manid=manga.id
            mPage.kkkid=ci.id
            mPage.name=ci.chid
            mPage.isbuckup=0
            mPage.ispush=0
            filepath="./tmp/image/%s/%s/"%(mPage.manid,mPage.kkkid)
            #生成epub
            mPage.size=self.createEpub(manga,ci,filepath)
            #获取该漫画的推送活保存权限
            man=self.db.getMangaByKkkid(manga.kkkid)
            epubpath="./tmp/image/%s/%s"%(manga.id,ci.id)
            try:
                with open("%s.mobi"%epubpath, 'rb') as e:
                    #开始发送邮件
                    if man.ispush==1:
                        msgRoot = MIMEMultipart('related')
                        msgRoot['Subject'] = "%s[%s][%s][%s]"%(manga.name,ci.chid,manga.author,manga.type)
                        msgRoot['From']=self.user.sendMail
                        msgRoot['To']=self.user.kindleMail
                        att = MIMEText(e.read(), 'base64', 'utf-8')
                        att["Content-Type"] = 'application/octet-stream'
                        att["Content-Disposition"] = 'attachment; filename="%s.mobi"'%ci.id
                        msgRoot.attach(att)
                        self.smtp.sendmail(self.user.sendMail, self.user.kindleMail, msgRoot.as_string())
                        mPage.ispush=1
            except Exception as e:
                print(e)
            
            try:
              with open("%s.zip"%epubpath, 'rb') as e:
                  #开始备份云盘与推送到kindle
                  if man.isbuckup==1:
                      ret = self.pcs.upload('/manga/[%s]%s'%(manga.author,manga.name),e,'%s.zip'%mPage.name)
                      mPage.isbuckup=1
            
              with open("%s.mobi"%epubpath, 'rb') as e:
                  #向云盘备份mobi
                  if man.isbuckup==1:
                      ret = self.pcs.upload('/manga/[%s]%s'%(manga.author,manga.name),e,'%s.mobi'%mPage.name)
              # 注册该漫画已完成下载,入库
              self.db.insertMangaPage(mPage)
              # 删除缓存文件
              os.remove("%s.mobi"%epubpath)
              os.remove("%s.zip"%epubpath)
              #删除目录
              shutil.rmtree(filepath)
            except Exception as e:
               print(e)




    def createEpub(self,manga,ci,path):
        #路径
        epubpath="./tmp/image/%s/%s"%(manga.id,ci.id)
        title="%s[%s][%s][%s]"%(manga.name,ci.chid,manga.author,manga.type)
        createKVBook(path,"%s.epub"%epubpath,title)
        #删除epub
        os.remove("%s.epub"%epubpath)
        #压缩path
        self.zip_dir(path,"%s.zip"%epubpath)
        return os.path.getsize("%s.mobi"%epubpath)

    def zip_dir(self,dirname,zipfilename):
        filelist = []
        if os.path.isfile(dirname):
            filelist.append(dirname)
        else :
            for root, dirs, files in os.walk(dirname):
                for name in files:
                    filelist.append(os.path.join(root, name))
        zf = zipfile.ZipFile(zipfilename, "w", zipfile.zlib.DEFLATED)
        for tar in filelist:
            arcname = tar[len(dirname):]
            zf.write(tar,arcname)
        zf.close()





class imagePojo:
    type=0
    name=""
    url=""

class User:
    id=0
    kindleMail=""
    sendMail=""
    sendMail_smtp=""
    sendMail_username=""
    sendMail_password=""
    baiduname=""
    baidupass=""

class Manga:
    id=0
    kkkid=0
    pageurl=""
    name=""
    state=""
    type=""
    author=""
    time=""
    isbuckup=1
    ispush=1
    ci=None

class MangaPage:
    hid=0
    manid=0
    kkkid=""
    name=""
    size=0
    isbuckup=1
    ispush=1

class MangaDao:
    def __init__(self):
        conn=sqlite3.connect('./manga.db')
        create="""
            CREATE TABLE IF NOT EXISTS 'user' ('id' INTEGER PRIMARY KEY,'kindleMail' VARCHAR,'sendMail' VARCHAR,'sendMail_smtp' VARCHAR,'sendMail_username' VARCHAR, 'sendMail_password' VARCHAR,'baiduname' VARCHAR,'baidupass' VARCHAR);
            CREATE TABLE IF NOT EXISTS 'manga' ('id' INTEGER PRIMARY KEY,'kkkid' INTEGER DEFAULT '0', pageurl VARCHAR, 'name' VARCHAR, 'state' INTEGER DEFAULT '1', 'type' VARCHAR, 'author' VARCHAR, 'time' VARCHAR, 'isbuckup' INTEGER DEFAULT '1', 'ispush' INTEGER DEFAULT '0');
            CREATE TABLE IF NOT EXISTS 'mangapage' ('hid' INTEGER PRIMARY KEY, 'manid' INTEGER,'kkkid' VARCHAR, 'name' VARCHAR, 'size' INTEGER, 'isbuckup' INTEGER, 'ispush' INTEGER);
            """
        conn.executescript(create)
        conn.commit()
        conn.close()

    def insertUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into user values(null,'%s','%s','%s','%s','%s','%s','%s')"%(user.kindleMail,user.sendMail,user.sendMail_smtp,user.sendMail_username,user.sendMail_password,user.baiduname,user.baidupass))
        conn.commit()
        conn.close()

    def deleteUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete user where id=%d"%user.id)
        conn.commit()
        conn.close()

    def updaetUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update user set kindleMail='%s' sendMail='%s' sendMail_smtp='%s' sendMail_username='%s' sendMail_password='%s' baiduname='%s' baidupass='%s'"%(user.baidukey,user.kindleMail,user.sendMail,user.sendMail_smtp,user.sendMail_username,user.sendMail_password,user.baiduname,user.baidupass))
        conn.commit()
        conn.close()

    def getUsers(self):
        conn=sqlite3.connect('./manga.db')
        cursor = conn.execute("select * from user")
        items=[]
        for i in cursor:
            user=User()
            user.id=i[0]
            user.kindleMail=i[1]
            user.sendMail=i[2]
            user.sendMail_smtp=i[3]
            user.sendMail_username=i[4]
            user.sendMail_password=i[5]
            user.baiduname=i[6]
            user.baidupass=i[7]
            items.append(user)
        conn.close()
        return items

    def getUserbyID(self,id):
        conn=sqlite3.connect('./manga.db')
        cursor = conn.execute("select * from user where id=%d"%id)
        items=[]
        for i in cursor:
            user=User()
            user.id=i[0]
            user.kindleMail=i[1]
            user.sendMail=i[2]
            user.sendMail_smtp=i[3]
            user.sendMail_username=i[4]
            user.sendMail_password=i[5]
            user.baiduname=i[6]
            user.baidupass=i[7]
            items.append(user)
        conn.close()
        return items[0]

    def insertManga(self,manga):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into manga values(null,'%s','%s',%s,'%s','%s','%s',%s,%s)"%(manga.kkkid,manga.pageurl,manga.name,manga.state,manga.type,manga.author,manga.time,manga.isbuckup,manga.ispush))
        conn.commit()
        conn.close()
    
    def insertMangaUrl(self,url):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into manga('pageurl') values('%s')"%url)
        conn.commit()
        conn.close()

    def updateManga(self,manga):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update manga set kkkid=%s,pageurl='%s',name='%s',state=%s,type='%s',author='%s',time='%s',isbuckup='%s',ispush='%s' where id=%s"%
            (manga.kkkid,manga.pageurl,manga.name,manga.state,manga.type,manga.author,manga.time,manga.isbuckup,manga.ispush,manga.id))
        conn.commit()
        conn.close()

    def delete(self,manga):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete manga where id=%d"%manga.id)
        conn.commit()
        conn.close()

    def getMangas(self):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from manga")
        items=[]
        for i in cursor:
            manga=Manga()
            manga.id=i[0]
            manga.kkkid=i[1]
            manga.pageurl=i[2]
            manga.name=i[3]
            manga.state=i[4]
            manga.type=i[5]
            manga.author=i[6]
            manga.time=i[7]
            manga.isbuckup=i[8]
            manga.ispush=i[9]
            items.append(manga)
        conn.close()
        return items

    def getMangaByid(self,id):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from manga where id=%d"%id)
        data=cursor.fetchall()
        if len(data)==0:
            return None
        items=[]
        for i in data:
            manga=Manga()
            manga.id=i[0]
            manga.kkkid=i[1]
            manga.pageurl=i[2]
            manga.name=i[3]
            manga.state=i[4]
            manga.type=i[5]
            manga.author=i[6]
            manga.time=i[7]
            manga.isbuckup=i[8]
            manga.ispush=i[9]
            items.append(manga)
        conn.close()
        return items[0]

    def getMangaByUrl(self,url):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from manga where pageurl='%s'"%url)
        data=cursor.fetchall()
        if len(data)==0:
            return None
        items=[]
        for i in data:
            manga=Manga()
            manga.id=i[0]
            manga.kkkid=i[1]
            manga.pageurl=i[2]
            manga.name=i[3]
            manga.state=i[4]
            manga.type=i[5]
            manga.author=i[6]
            manga.time=i[7]
            manga.isbuckup=i[8]
            manga.ispush=i[9]
            items.append(manga)
        conn.close()
        return items[0]

    def getMangaByKkkid(self,url):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from manga where kkkid='%s'"%url)
        data=cursor.fetchall()
        if len(data)==0:
            return None
        items=[]
        for i in data:
            manga=Manga()
            manga.id=i[0]
            manga.kkkid=i[1]
            manga.pageurl=i[2]
            manga.name=i[3]
            manga.state=i[4]
            manga.type=i[5]
            manga.author=i[6]
            manga.time=i[7]
            manga.isbuckup=i[8]
            manga.ispush=i[9]
            items.append(manga)
        conn.close()
        return items[0]

    def insertMangaPage(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into mangapage values(null,'%d','%s','%s','%d','%d','%d')"%(mangapage.manid,mangapage.kkkid,mangapage.name,mangapage.size,mangapage.isbuckup,mangapage.ispush))
        conn.commit()
        conn.close()

    def updateMangaPage(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update mangapage set manid=%d,kkkid='%s',name='%s',size='%d',isbuckup='%d',ispush='%d' where hid=%d"%(mangapage.manid,mangapage.kkkid,mangapage.name,mangapage.size,mangapage.isbuckup,mangapage.ispush,mangapage.hid))
        conn.commit()
        conn.close()

    def updateMangaPageBykkkid(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update mangapage set manid=%d,name='%s',size='%d',isbuckup='%d',ispush='%d' where kkkid=%s"%(mangapage.manid,mangapage.name,mangapage.size,mangapage.isbuckup,mangapage.ispush,mangapage.kkkid))
        conn.commit()
        conn.close()

    def deleteMangaPage(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete mangapage where hid=%d"%mangapage.hid)
        conn.commit()
        conn.close()

    def deleteMangaPageByMan(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete mangapage where manid=%d"%mangapage.manid)
        conn.commit()
        conn.close()

    def getMangaPageByid(self,id):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from mangapage where hid=%d"%id)
        data=cursor.fetchall()
        if len(data)==0:
            return None
        items=[]
        for i in data:
            mangapage=MangaPage()
            mangapage.hid=i[0]
            mangapage.manid=i[1]
            mangapage.kkkid=i[2]
            mangapage.name=i[3]
            mangapage.size=i[4]
            mangapage.isbuckup=i[5]
            mangapage.ispush=i[6]
            items.append(mangapage)
        conn.close()
        return items[0]

    def getMangaPageByMan(self,id):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from mangapage where manid=%d"%id)
        items=[]
        for i in cursor:
            mangapage=MangaPage()
            mangapage.hid=i[0]
            mangapage.manid=i[1]
            mangapage.kkkid=i[2]
            mangapage.name=i[3]
            mangapage.size=i[4]
            mangapage.isbuckup=i[5]
            mangapage.ispush=i[6]
            items.append(mangapage)
        conn.close()
        return items

    def getMangaPageByKkkid(self,id):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select * from mangapage where kkkid='%s'"%id)
        data=cursor.fetchall()
        if len(data)==0:
            return None
        items=[]
        for i in data:
            mangapage=MangaPage()
            mangapage.hid=i[0]
            mangapage.manid=i[1]
            mangapage.kkkid=i[2]
            mangapage.name=i[3]
            mangapage.size=i[4]
            mangapage.isbuckup=i[5]
            mangapage.ispush=i[6]
            items.append(mangapage)
        conn.close()
        return items[0]

    def getMangaMaxID(self,url):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select max(t1.kkkid) as max from mangapage t1 left join manga t2 on t1.manid=t2.id where t2.pageurl=''"%url)
        data=cursor.fetchall()
        if len(data)==0:
            return 0
        max=data[0][0]
        conn.close()
        return max

