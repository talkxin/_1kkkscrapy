10# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import os
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
from _1kkk.items import KkkItem


class KkkPipeline(object):
    
    def open_spider(self, spider):
        #初始化邮箱
        db=MangaDao()
        #默认读取首位用户
        self.user=db.getUserbyID(1)
        self.smtp = smtplib.SMTP()
        self.smtp.connect(self.user.sendMail_smtp)
        self.smtp.login(self.user.sendMail_username, self.user.sendMail_password)
        
        #初始化队列文件
        self.man=downloadImage()
        self.man.start()
    
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
        self.man.put(item)
        return item


class downloadImage(threading.Thread):
    def __init__(self):
        self.db=MangaDao()
        self.queue=queue.Queue(0)
        super().__init__()
    
    def put(self,items):
        self.queue.put(items)
    
    def run(self):
        while True:
            items=self.queue.get()
            if(isinstance(items,KkkItem)):
                self.initManga(items)
            else:
                break
    

    def initManga(self,items):
        url=items['url']
        manga=self.db.getMangaByUrl(url)
        manga.kkkid=items['id']
        manga.name=items['name']
        if self.item['state']=="连载中":
            manga.state=1
        else:
            manga.state=0
        manga.type=items['type']
        manga.time=items['time']
        manga.author=items['author']
        self.db.updateManga(manga)
        for ci in items['chapter']:
            mPage=MangaPage()
            mPage.manid=manga.id
            mPage.kkkid=ci.id
            mPage.name=ci.chid
            mPage.isbuckup=0
            mPage.ispush=0
            filepath="./tmp/image/%s/%s/"%(mPage.manid,mPage.kkkid)
            #生成epub
            mPage.size=self.createEpub(manga,ci,filepath)
            #注册该漫画已完成下载,入库
            self.db.insertMangaPage(mPage)
            #获取该漫画的推送活保存权限
            man=self.db.getMangaPageByMan(manga.manid)
            epubpath="./tmp/image/%s/%s.epub"%(manga.id,ci.id)
            if man.isbuckup==1:
            #开始备份云盘与推送到kindle
                print("save")
            #开始发送邮件
            if man.ispush==1:
                with open(epubpath, 'rb') as e:
                    msgRoot = MIMEMultipart('related')
                    msgRoot['Subject'] = mPage.kkkid
                    att = MIMEText(e.read(), 'base64', 'utf-8')
                    att["Content-Type"] = 'application/octet-stream'
                    att["Content-Disposition"] = 'attachment; filename="%s.epub"'%ci.id
                    msgRoot.attach(att)
                    smtp.sendmail(self.user.sendMail, self.user.kindleMail, msgRoot.as_string())




    def createEpub(self,manga,ci,path):
        #路径
        epubpath="./tmp/image/%s/%s.epub"%(manga.id,ci.id)
        title="%s[%s][%s][%s]"%(manga.name,ci.chid,manga.author,manga.type)
        createKVBook(path,epubpath,title)
        shutil.rmtree(path)
        return os.path.getsize(epubpath)
#        book = epub.EpubBook()
#        #绝对ID
#        book.set_identifier(ci.id)
#        #书籍名称
#        book.set_title("%s[%s][%s][%s]"%(manga.name,ci.chid,manga.author,manga.type))
#        #语言
#        book.set_language('en')
#        #作者
#        book.add_author('talkxin')
#        book.spine=[]
#        #封面
#        book.set_cover("image.jpg",open('%s/1.jpg'%path, 'rb').read())
#        toc=[]
#        for i in range(1,len(ci.page)+1):
#            itm = epub.EpubImage()
#            itm.file_name ="%s.jpg"%i
#            itm.content=open('%s/%s.jpg'%(path,i), 'rb').read()
#            book.add_item(itm)
#            toc.append(epub.Link("%s.jpg"%i,"p%s"%i,"p%s"%i))
#            book.spine.append(itm)
#            os.remove('%s/%s.jpg'%(path,i))
#        #目录
#        book.toc = (toc)
#        book.add_item(epub.EpubNcx())
#        book.add_item(epub.EpubNav())
#        #路径
#        path="./tmp/image/%s/%s.epub"%(manga.id,ci.id)
#        epub.write_epub(path, book, {})
#        return os.path.getsize(path)

    def compressionMobi(self,path,ci):
#        os.popen('./bin/kindlegen_mac_i386_v2_9 %s/%s.epub -c2'%(path,ci.id)).readlines()
        #进行压缩
        data=open('%s/%s.mobi'%(path,ci.id), 'rb').read()
        os.remove('%s/%s.mobi'%(path,ci.id))
        out=SRCSStripper(data)
        open('%s/%s-1.mobi'%(path,ci.id),'wb').write(out.getResult())





class imagePojo:
    type=0
    name=""
    url=""

class User:
    id=0
    baidukey=""
    kindleMail=""
    sendMail=""
    sendMail_smtp=""
    sendMail_username=""
    sendMail_password=""

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
    isbuckup=1
    ispush=1

class MangaDao:
    def __init__(self):
        conn=sqlite3.connect('./manga.db')
        create="""
            CREATE TABLE IF NOT EXISTS 'user' ('id' INTEGER PRIMARY KEY, 'baidukey' VARCHAR,'kindleMail' VARCHAR,'sendMail' VARCHAR,'sendMail_smtp' VARCHAR,'sendMail_username' VARCHAR, 'sendMail_password' VARCHAR );
            CREATE TABLE IF NOT EXISTS 'manga' ('id' INTEGER PRIMARY KEY,'kkkid' INTEGER DEFAULT '0', pageurl VARCHAR, 'name' VARCHAR, 'state' INTEGER DEFAULT '1', 'type' VARCHAR, 'author' VARCHAR, 'time' VARCHAR, 'isbuckup' INTEGER DEFAULT '1', 'ispush' INTEGER DEFAULT '1');
            CREATE TABLE IF NOT EXISTS 'mangapage' ('hid' INTEGER PRIMARY KEY, 'manid' INTEGER,'kkkid' VARCHAR, 'name' VARCHAR, 'size' INTEGER, 'isbuckup' INTEGER, 'ispush' INTEGER);
            """
        conn.executescript(create)
        conn.commit()
        conn.close()

    def insertUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("insert into user values(null,'%s','%s','%s','%s','%s','%s')"%(user.baidukey,user.kindleMail,user.sendMail,user.sendMail_smtp,user.sendMail_username,user.sendMail_password))
        conn.commit()
        conn.close()

    def deleteUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("delete user where id=%d"%user.id)
        conn.commit()
        conn.close()

    def updaetUser(self,user):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update user set baidukey='%s' kindleMail='%s' sendMail='%s' sendMail_smtp='%s' sendMail_username='%s' sendMail_password='%s'"%(user.baidukey,user.kindleMail,user.sendMail,user.sendMail_smtp,user.sendMail_username,user.sendMail_password))
        conn.commit()
        conn.close()

    def getUsers(self):
        conn=sqlite3.connect('./manga.db')
        cursor = conn.execute("select * from user")
        items=[]
        for i in cursor:
            user=User()
            user.id=i[0]
            user.baidukey=i[1]
            user.kindleMail=i[2]
            user.sendMail=i[3]
            user.sendMail_smtp=i[4]
            user.sendMail_username=i[5]
            user.sendMail_password=i[6]
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
            user.baidukey=i[1]
            user.kindleMail=i[2]
            user.sendMail=i[3]
            user.sendMail_smtp=i[4]
            user.sendMail_username=i[5]
            user.sendMail_password=i[6]
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
        conn.execute("insert into mangapage values(null,'%d','%s','%s','%d','%d','%d')"%(mangapage.manid,mangapage.kkkid,mangapage.name,mangapage.size,mangapage.isbuckup,mangapage.ispush))
        conn.commit()
        conn.close()

    def updateMangaPage(self,mangapage):
        conn=sqlite3.connect('./manga.db')
        conn.execute("update mangapage set manid=%d,kkkid='%s',name='%s',size='%d',isbuckup='%d',ispush='%d' where hid=%d"%(mangapage.manid,mangapage.kkkid,mangapage.name,mangapage.size,mangapage.isbuckup,mangapage.ispush,mangapage.hid))
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

class SRCSStripper:
    data_file=b""
    def sec_info(self, secnum):
        start_offset, flgval = struct.unpack_from('>2L', self.datain, 78+(secnum*8))
        if secnum == self.num_sections:
            next_offset = len(self.datain)
        else:
            next_offset, nflgval = struct.unpack_from('>2L', self.datain, 78+((secnum+1)*8))
        return start_offset, flgval, next_offset

    def loadSection(self, secnum):
        start_offset, tval, next_offset = self.sec_info(secnum)
        return self.datain[start_offset: next_offset]


    def __init__(self, datain):
        if datain[0x3C:0x3C+8] != b'BOOKMOBI':
            return None
        self.datain = datain
        self.num_sections, = struct.unpack('>H', datain[76:78])

        # get mobiheader
        mobiheader = self.loadSection(0)

        # get SRCS section number and count
        self.srcs_secnum, self.srcs_cnt = struct.unpack_from('>2L', mobiheader, 0xe0)
        if self.srcs_secnum == 0xffffffff or self.srcs_cnt == 0:
            raise StripException("File doesn't contain the sources section.")

        # store away srcs sections in case the user wants them later
        self.srcs_headers = []
        self.srcs_data = []
        for i in range(self.srcs_secnum, self.srcs_secnum + self.srcs_cnt):
            data = self.loadSection(i)
            self.srcs_headers.append(data[0:16])
            self.srcs_data.append(data[16:])

        # find its SRCS region starting offset and total length
        self.srcs_offset, fval, temp2 = self.sec_info(self.srcs_secnum)
        next = self.srcs_secnum + self.srcs_cnt
        next_offset, temp1, temp2 = self.sec_info(next)
        self.srcs_length = next_offset - self.srcs_offset

        if self.datain[self.srcs_offset:self.srcs_offset+4] != b'SRCS':
            return None

        # first write out the number of sections
        self.data_file = self.datain[:76]
        self.data_file = self.joindata(self.data_file, struct.pack('>H',self.num_sections))

        # we are going to make the SRCS section lengths all  be 0
        # offsets up to and including the first srcs record must not be changed
        last_offset = -1
        for i in range(self.srcs_secnum+1):
            offset, flgval, temp  = self.sec_info(i)
            last_offset = offset
            self.data_file = self.joindata(self.data_file, struct.pack('>L',offset) + struct.pack('>L',flgval))
            # print "section: %d, offset %0x, flgval %0x" % (i, offset, flgval)

        # for every additional record in SRCS records set start to last_offset (they are all zero length)
        for i in range(self.srcs_secnum + 1, self.srcs_secnum + self.srcs_cnt):
            temp1, flgval, temp2 = self.sec_info(i)
            self.data_file = self.joindata(self.data_file, struct.pack('>L',last_offset) + struct.pack('>L',flgval))
            # print "section: %d, offset %0x, flgval %0x" % (i, last_offset, flgval)

        # for every record after the SRCS records we must start it earlier by an amount
        # equal to the total length of all of the SRCS section
        delta = 0 - self.srcs_length
        for i in range(self.srcs_secnum + self.srcs_cnt , self.num_sections):
            offset, flgval, temp = self.sec_info(i)
            offset += delta
            self.data_file = self.joindata(self.data_file, struct.pack('>L',offset) + struct.pack('>L',flgval))
            # print "section: %d, offset %0x, flgval %0x" % (i, offset, flgval)

        # now pad it out to begin right at the first offset
        # typically this is 2 bytes of nulls
        first_offset, flgval = struct.unpack_from('>2L', self.data_file, 78)
        self.data_file = self.joindata(self.data_file, '\0' * (first_offset - len(self.data_file)))

        # now add on every thing up to the original src_offset and then everything after it
        dout = []
        dout.append(self.data_file)
        dout.append(self.datain[first_offset: self.srcs_offset])
        dout.append(self.datain[self.srcs_offset+self.srcs_length:])
        self.data_file = b"".join(dout)

        # update the srcs_secnum and srcs_cnt in the new mobiheader
        offset0, flgval0 = struct.unpack_from('>2L', self.data_file, 78)
        offset1, flgval1 = struct.unpack_from('>2L', self.data_file, 86)
        mobiheader = self.data_file[offset0:offset1]
        mobiheader = mobiheader[:0xe0]+ struct.pack('>L', 0xffffffff) + struct.pack('>L', 0) + mobiheader[0xe8:]
        self.data_file = self.patchdata(self.data_file, offset0, mobiheader)
        return None

    def getResult(self):
        return self.data_file

    def getStrippedData(self):
        return self.srcs_data

    def getHeader(self):
        return self.srcs_headers

    def joindata(self,datain, new):
        dout=[]
        if(isinstance(datain,str)):
            datain=bytes(datain,'utf-8')
        if(isinstance(new,str)):
            new=bytes(new,'utf-8')
        dout.append(datain)
        dout.append(new)
        return b''.join(dout)

    def patchdata(self,datain, off, new):
        dout=[]
        dout.append(datain[:off])
        dout.append(new)
        dout.append(datain[off+len(new):])
        return b''.join(dout)


