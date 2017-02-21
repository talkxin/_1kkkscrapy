10# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import os,os.path
import zipfile
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
from _1kkk.libs.kcc.kcc.comic2ebook import createKVBook
from _1kkk.libs.baidupcsapi.baidupcsapi import PCS
from _1kkk.items import KkkItem
from _1kkk.items import Chapter
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed


class KkkPipeline(object):

    def open_spider(self, spider):
        #初始化队列文件
        self.man=downloadImage()
        self.man.start()
        self.db=MangaDao()
        time.sleep(10)
        for i in self.db.getNotBackupManga():
            self.man.put(i)
        #优先处理未解决的漫画
        while len(self.db.getNotBackupManga())!=0:
            time.sleep(5)
            continue


    def close_spider(self, spider):
        self.man.close()

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

        mPage=MangaPage()
        mPage.manid=manga.id
        mPage.kkkid=manga.ci.id
        mPage.name=manga.ci.chid
        mPage.isbuckup=0
        mPage.ispush=0
        # 文件下载完成入库
        self.db.insertMangaPage(mPage)

        self.man.put(manga)
        return item


class downloadImage(threading.Thread):
    def __init__(self):
        self.db=MangaDao()
        #默认读取首位用户
        self.user=self.db.getUserbyID(1)

        try:
            self.smtp = smtplib.SMTP()
            self.smtp.connect(self.user.sendMail_smtp)

            #登陆smtp
            if self.user.sendMail_username!=None and self.user.sendMail_username!="" and self.user.sendMail_password!=None and self.user.sendMail_password!="":
                self.smtp.login(self.user.sendMail_username, self.user.sendMail_password)
        except Exception as e:
            print(e)

        self.pcs = PCS(self.user.baiduname,self.user.baidupass)
        while json.loads(self.pcs.quota().content.decode())['errno']==-6:
            time.sleep(3)
            self.pcs = PCS(self.user.baiduname,self.user.baidupass)

        #三部漫画同时处理
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.tq=[]

        #是否退出
        self.closeKey=True

        super().__init__()

    def put(self,items):
        self.tq.append(self.executor.submit(self.initManga,items))

    def close(self):
        self.closeKey=False

    def run(self):
        while self.closeKey:
            for k,v in enumerate(as_completed(self.tq)):
                if v.result():
                    del self.tq[k]


    def initManga(self,items):
        manga=items
        ci=manga.ci
        mPage=self.db.getMangaPageByKkkid(manga.ci.id)
        filepath="./tmp/image/%s/%s/"%(mPage.manid,mPage.kkkid)
        #目录不存在
        if not os.path.exists(filepath):
            #删除数据库条目，尝试再次下载
            self.db.deleteMangaPageBykkkid(mPage)
            return
        try:
            #生成epub
            mPage.size=self.createEpub(manga,ci,filepath)
            logging.info("===============================")
        except Exception as e:
            logging.info(filepath)
            #出现错误，回滚
            logging.error(str(e))
            #删除目录
            shutil.rmtree(filepath)
            #删除数据库条目，尝试再次下载
            self.db.deleteMangaPageBykkkid(mPage)
            return

        #获取该漫画的推送活保存权限
        man=self.db.getMangaByKkkid(manga.kkkid)
        epubpath="./tmp/image/%s/%s"%(manga.id,ci.id)
        try:
            if man.ispush==1:
                #开始发送邮件
                msgRoot = MIMEMultipart('related')
                msgRoot['Subject'] = "%s[%s][%s][%s]"%(manga.name,ci.chid,manga.author,manga.type)
                msgRoot['From']=self.user.sendMail
                msgRoot['To']=self.user.kindleMail
                #超过50mb的附件提醒自行下载
                if mPage.size<50*1024*1024:
                    with open("%s.mobi"%epubpath, 'rb') as e:
                        att = MIMEText(e.read(), 'base64', 'utf-8')
                        att["Content-Type"] = 'application/octet-stream'
                        att["Content-Disposition"] = 'attachment; filename="%s.mobi"'%ci.id
                        msgRoot.attach(att)
                        self.smtp.sendmail(self.user.sendMail, self.user.kindleMail, msgRoot.as_string())
                        mPage.ispush=1
                else:
                    file_object = open('%s.txt'%epubpath, 'w')
                    file_object.write("%s[%s][%s][%s] is too big,please open the baidu cloud,download this file"%(manga.name,ci.chid,manga.author,manga.type))
                    file_object.close
                    with open("%s.txt"%epubpath, 'rb') as e:
                        att = MIMEText(e.read(), 'base64', 'utf-8')
                        att["Content-Type"] = 'application/octet-stream'
                        att["Content-Disposition"] = 'attachment; filename="%s.mobi"'%ci.id
                        msgRoot.attach(att)
                        self.smtp.sendmail(self.user.sendMail, self.user.kindleMail, msgRoot.as_string())
                        mPage.ispush=0
                        #删除临时文件
                        os.remove("%s.txt"%epubpath)
                        #修改属性
                        self.db.updateMangaPageBykkkid(mPage)
        except Exception as e:
            logging.warning(str(e))

        try:
            if man.isbuckup==1:
                logging.info("into cloud")
                with open("%s.zip"%epubpath, 'rb') as e:
                    #向云盘备份图片源文件打包zip
                    logging.info("start upload %s zip"%manga.name)
                    ret = self.pcs.upload('/manga/[%s][%s]%s/zip'%(manga.type,manga.author,manga.name),e,'%s.zip'%mPage.name)
                    logging.info("end upload %s zip"%manga.name)
                    mPage.isbuckup=1

                with open("%s.mobi"%epubpath, 'rb') as e:
                    #向云盘备份mobi
                    logging.info("start upload %s mobi"%manga.name)
                    ret = self.pcs.upload('/manga/[%s][%s]%s/mobi'%(manga.type,manga.author,manga.name),e,'%s.mobi'%mPage.name)
                    logging.info("end upload %s mobi"%manga.name)

                logging.info("end cloud")
            # 删除缓存文件
            os.remove("%s.mobi"%epubpath)
            os.remove("%s.zip"%epubpath)
            #删除目录
            shutil.rmtree(filepath)
            #修改属性
            self.db.updateMangaPageBykkkid(mPage)
        except Exception as e:
            logging.warning(str(e))


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
        conn.execute("delete from user where id=%d"%user.id)
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
        conn.execute("delete from manga where id=%d"%manga.id)
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
        conn.execute("delete from mangapage where hid=%d"%mangapage.hid)
        conn.commit()
        conn.close()

    def deleteMangaPageByMan(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete from mangapage where manid=%d"%mangapage.manid)
        conn.commit()
        conn.close()

    def deleteMangaPageBykkkid(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete from mangapage where kkkid=%s"%mangapage.kkkid)
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

    def getNotBackupManga(self):
        conn=sqlite3.connect('./manga.db')
        cursor=conn.execute("select t2.*,t1.name,t1.kkkid from mangapage as t1 left join manga as t2 on t1.manid=t2.id where t2.isbuckup=1 and t1.isbuckup=0")
        items=[]
        data=cursor.fetchall()
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
            ci=Chapter()
            ci.chid=i[10]
            ci.id=i[11]
            manga.ci=ci
            items.append(manga)
        conn.close()
        return items
