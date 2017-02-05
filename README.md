0、安装所需要的支持库：
	apt-get update
	apt-get install -y wget vim gcc g++ git lrzsz build-essential libxml2 libxml2-dev openssl libssl-dev libffi-dev libtiff5-dev libjpeg8-dev zlib1g-dev libfreetype6-dev liblcms2-dev libwebp-dev libfontconfig python python-dev python3 python3-dev python-setuptools python3-setuptools python3-pip python3-openssl python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev libffi-dev libssl-dev

1、安装scrapy及所需的python库：
	pip3 install pip PyExecJS pyasn1 cryptography python-slugify Pillow psutil selenium requests_toolbelt rsa scrapy --upgrade
	
2、下载项目：
	git clone https://github.com/talkxin/_1kkkscrapy.git

3、试运行项目，初始化数据库：
	scrapy crawl manhua

4、更新数据库user表及manga表：
	1）更新user表中的百度账号密码，kindle推送邮箱及关联推送邮箱
	2）将需要推送的漫画url添加至manga表中的pageurl列，等待挖掘成功后，会自动填充其他列数据

5、开始运行脚本：
	运行run.sh脚本，开始下载漫画并进行推送

6、将脚本加入至crontab中进行定时挖掘：
	1）执行crontab -e
	2）将“0 0 * * * sh /home/xxx/run.sh”添加至文本最后一行，路径自行修改，该行代表每天运行一次，检查漫画更新，可自行调整间隔。
