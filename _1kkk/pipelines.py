10# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import os
import urllib.request#python3
import threading
import queue
import time
import sqlite3


class KkkPipeline(object):
    
    """
        每个漫画创建一个下载线程进行下载
        下载完成后生成epub格式的书籍进行备份
        同时将epub格式的书籍与百度云盘进行同步
        并且根据user表中定义的用户，将数据推送至该用户的kindle
    """
    def process_item(self, item, spider):
        print("into pipe")
        man=downloadImage(item)
        man.start()
        return item


class downloadImage(threading.Thread):

    def __init__(self,items):
        self.item=items
        self.db=MangaDao()
        super().__init__()

    def run(self):
        url=self.item['url']
        manga=self.db.getMangaByUrl(url)
        manga.kkkid=self.item['id']
        manga.name=self.item['name']
        if self.item['state']=="连载中":
            manga.state=1
        else:
            manga.state=0
        manga.type=self.item['type']
        manga.time=self.item['time']
        manga.author=self.item['author']
        self.db.updateManga(manga)
        for ci in self.item['chapter']:
            mPage=MangaPage()
            mPage.manid=manga.id
            mPage.kkkid=ci.id
            mPage.name=ci.chid
            filepath="./tmp/image/%s/%s/"%(mPage.manid,mPage.kkkid)
            if os.path.exists(filepath) != True:
                os.makedirs(filepath)
            for i in ci.page:
                urllib.request.urlretrieve(i.imageurl, "./tmp/image/%s/%s/%s.jpg"%(mPage.manid,mPage.kkkid,i.id))
            self.db.insertMangaPage(mPage)

class imagePojo:
    type=0
    name=""
    url=""

class User:
    id=0
    email=""
    baidukey=""

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

class MangaPage:
    hid=0
    manid=0
    kkkid=""
    name=""
    size=0

class MangaDao:
    def __init__(self):
        conn=sqlite3.connect('./manga.db')
        create="""
            CREATE TABLE IF NOT EXISTS 'user' ('id' INTEGER PRIMARY KEY, 'email' VARCHAR, 'baidukey' VARCHAR);
            CREATE TABLE IF NOT EXISTS 'manga' ('id' INTEGER PRIMARY KEY,'kkkid' INTEGER DEFAULT '0', pageurl VARCHAR, 'name' VARCHAR, 'state' INTEGER, 'type' VARCHAR, 'author' VARCHAR, 'time' VARCHAR, 'isbuckup' INTEGER DEFAULT '1', 'ispush' INTEGER DEFAULT '1');
            CREATE TABLE IF NOT EXISTS 'mangapage' ('hid' INTEGER PRIMARY KEY, 'manid' INTEGER,kkkid VARCHAR, 'name' VARCHAR, 'size' INTEGER);
            """
        conn.executescript(create)
        conn.commit()
        conn.close()

    def insertUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into user values(null,'%s','%s')"%(user.email,user.baidukey))
        conn.commit()
        conn.close()

    def deleteUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete user where id=%d"%user.id)
        conn.commit()
        conn.close()

    def updaetUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update user set email='%s',baidukey='%s'"%(user.email,user.baidukey))
        conn.commit()
        conn.close()

    def getUsers(self):
        conn=sqlite3.connect('./manga.db')
        cursor = conn.execute("select * from user")
        items=[]
        for i in cursor:
            user=User()
            user.id=i[0]
            user.email=i[1]
            user.baidukey=i[2]
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
            user.email=i[1]
            user.baidukey=i[2]
            items.append(user)
        conn.close()

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

    def insertMangaPage(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into mangapage values(null,'%d','%s','%s','%d')"%(mangapage.manid,mangapage.kkkid,mangapage.name,mangapage.size))
        conn.commit()
        conn.close()

    def updateMangaPage(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update mangapage set manid=%d,kkkid='%s',name='%s',size=%d where hid=%d"%(mangapage.manid,mangapage.kkkid,mangapage.name,mangapage.size,mangapage.hid))
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
            items.append(mangapage)
        conn.close()
        return items[0]


