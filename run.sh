#!/bin/sh

export PATH=$PATH:/usr/local/bin

cd /root/_1kkkscrapy/
# scrapy=`ps -C scrapy | awk 'NR==2{print $1}'`
# if [ ! $scrapy ]; then
# nohup scrapy crawl manhua >> example.log 2>&1 &
# fi
for i in `ps -C scrapy | awk 'NR>=2{print $1}'`
do
  kill -9 $i
done

nohup scrapy crawl manhua >> example.log 2>&1 &
