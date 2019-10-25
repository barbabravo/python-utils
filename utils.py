import re
import os, os.path
import cx_Oracle
import pymysql
import pymongo

import configparser

from datetime import datetime, timezone, timedelta
import time
import math
import arrow
import sys
import copy
import xlsxwriter
import logging
import logging.config
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from phone import Phone

import smtplib

import requests
import json

import jpush

import zipfile


# app_key=
# master_secret=

# _jpush = jpush.JPush(app_key, master_secret)
# _jpush.set_logging("warning")


cur_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(cur_path,'env.conf')
conf = configparser.ConfigParser()
conf.read(config_path,encoding='utf-8-sig')
 
os.environ['NLS_LANG'] = conf.get('global', 'NLS_LANG')
env = conf.get('global', 'env')


depositServiceConfig = {
    'db': conf.get('deposit_service', 'db'),
    'host': conf.get("deposit_service", "host"),
    'port': int(conf.get("deposit_service", "port")),
    'sid' : conf.get("deposit_service", "sid"),
    'user': conf.get("deposit_service", "user"),
    'password' : conf.get("deposit_service", "password")
}

metisServiceConfig = {
    'db': conf.get('metis_service', 'db'),
    'host': conf.get("metis_service", "host"),
    'port': int(conf.get("metis_service", "port")),
    'sid' : conf.get("metis_service", "sid"),
    'user': conf.get("metis_service", "user"),
    'password' : conf.get("metis_service", "password")
}

ecjiaServiceConfig = {
    'db': conf.get('ecjia_service', 'db'),
    'host': conf.get("ecjia_service", "host"),
    'port': int(conf.get("ecjia_service", "port")),
    'user': conf.get("ecjia_service", "user"),
    'password' : conf.get("ecjia_service", "password"),
    'database' : conf.get("ecjia_service", "database")
}

vmsServiceConfig = {
    'db': conf.get('vms_service', 'db'),
    'host': conf.get("vms_service", "host"),
    'port': int(conf.get("vms_service", "port")),
    'user': conf.get("vms_service", "user"),
    'password' : conf.get("vms_service", "password"),
    'database' : conf.get("vms_service", "database")
}


logging.config.fileConfig('logging.conf')
logger = logging.getLogger(name="root")

def getLogger():
    return logger

def getConnection(connectionParameters):
    _conn = None
    db = connectionParameters['db']
    if db == 'oracle':
        dsnStr = cx_Oracle.makedsn(connectionParameters['host'], connectionParameters['port'], connectionParameters['sid'])
        _conn = cx_Oracle.connect(user=connectionParameters['user'], password=connectionParameters['password'], dsn=dsnStr)
    elif db == 'mysql':
        _conn = pymysql.connect(host=connectionParameters['host'], port=connectionParameters['port'],user=connectionParameters['user'],password=connectionParameters['password'],db=connectionParameters['database'],charset="utf8")
    else:
        _conn = None
    return _conn

def getConnectionByServiceName(serviceName):
    _conn = None
    try:
        if serviceName == 'metis_service':
            _conn = getConnection(metisServiceConfig)
        elif serviceName == 'ecjia_service':
            _conn = getConnection(ecjiaServiceConfig)
        elif serviceName == 'deposit_service':
            _conn = getConnection(depositServiceConfig)
        elif serviceName == 'vms_service':
            _conn = getConnection(vmsServiceConfig)
        else:
            _conn = None
    except Exception as err:
        logger.error('获取数据库%s连接失败'%(serviceName))
        logger.error(err)
    return _conn

def closeConnection(conn):
    try:
        conn.close()
    except Exception as err:
        logger.info('关闭数据库连接失败.')
        logger.error(err)

'''
example:
wb = utils.create_worksheet(fileName, fields, dataList, '表名1')
utils.add_worksheet(wb, fields, dataList, '表名2')
utils.add_worksheet(wb, fields, dataList, '表名3')
utils.save_worksheet(wb)
'''

def create_worksheet(fileName, fields, dataList,  sheetName=''):

    wb = xlsxwriter.Workbook(fileName)
    #创建一个sheet
    sheet = wb.add_worksheet(sheetName)

    dataStartRow = 0

    #插入列名
    for i in range(0, len(fields)):
        sheet.write(dataStartRow,i,fields[i]) 
    dataStartRow = dataStartRow + 1

    #将数据插入表格
    for i in range(0, len(dataList)):
        for j in range(len(fields)):
            sheet.write(i+dataStartRow, j, dataList[i][fields[j]])
    
    return wb

def add_worksheet(wb,fields, dataList, sheetName=''):

    sheet = wb.add_worksheet(sheetName)

    dataStartRow = 0

    # 插入列名
    for i in range(0, len(fields)):
        sheet.write(dataStartRow, i, fields[i])
    dataStartRow = dataStartRow + 1

    # 将数据插入表格
    for i in range(0, len(dataList)):
        for j in range(len(fields)):
            sheet.write(i + dataStartRow, j, dataList[i][fields[j]])

def save_worksheet(wb):
    wb.close()

def writeFile(filePath, data):
    fo = open(filePath, 'w')
    fo.write(str(data))
    fo.close()

def create_email(email_from, email_to, email_cc, email_Subject, email_text, files=[]):
    #生成一个空的带附件的邮件实例
    message = MIMEMultipart()
    #将正文以text的形式插入邮件中
    message.attach(MIMEText(email_text, 'plain', 'utf-8'))
    #生成邮件主题
    message['Subject'] = Header(email_Subject, 'utf-8')
    #生成发件人名称（这个跟发送的邮件没有关系）
    message['From'] = Header(email_from, 'utf-8')
    #生成收件人名称（这个跟接收的邮件也没有关系）
    message['To'] = ','.join(email_to)
    if email_cc:
        message['Cc'] = ','.join(email_cc)

    #读取附件的内容
    if (len(files)) != 0:
        for i in range(len(files)):
            if os.path.isfile(files[i]):
                fileName = files[i].split('/')[-1]
                att = MIMEText(open(files[i], 'rb').read(), 'base64', 'utf-8')
                att["Content-Type"] = 'application/octet-stream'
                #生成附件的名称
                att.add_header('Content-Disposition', 'attachment', fileName=fileName)
                message.attach(att)
                #xlsx类型附件

    #返回邮件
    return message

def send_email(smtp_host, smtp_port, sender, password, receiver, msg):
    try:
        server=smtplib.SMTP_SSL(smtp_host, smtp_port)
        server.ehlo()
        server.login(sender, password)
        #发送邮件
        server.sendmail(sender, receiver, msg.as_string())
        server.quit()  # 关闭连接
    except Exception as err:
        logger.info('发送邮件失败.')
        logger.error(err)


def to_date(timestamp, fmt):
    timeArray = time.localtime(timestamp + 8*3600)
    return time.strftime(fmt, timeArray)

def to_timestamp(dt_str, tz_str):
    if(dt_str == None):
        return dt_str
    dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S') # str-->datetime
    tz_r = re.match(r'^UTC([+|-]\d{1,2}):00$', tz_str) #UTC中获取时区信息
    tz = timezone(timedelta(hours=int(tz_r.group(1)))) # 创建时区UTC
    dt = dt.replace(tzinfo=tz) # 利用tzinfo属性将datetime强制设置成指定时区
    return dt.timestamp() # 返回timestamp
    
def getCurrentDate():
    return time.strftime("%Y-%m-%d", time.localtime())

def getCurrentTime():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    
def getdate(beforeOfDay):
        today = datetime.now()
        # 计算偏移量
        offset = timedelta(days=-beforeOfDay)
        # 获取想要的日期的时间
        re_date = (today + offset).strftime('%Y-%m-%d')
        return re_date
         
def getMonth(beforeOfMonth):
        targetMonth = arrow.now().shift(months=beforeOfMonth)
        # 获取想要的月份
        re_date = targetMonth.strftime('%Y-%m')

        return re_date

'''
判断手机号来源
'''
def getPhoneInfo(phoneNum):
    info = Phone().find(phoneNum)
    if info:
        phone = info['phone']
        province = info['province']
        city = info['city']
        zip_code = info['zip_code']
        area_code = info['area_code']
        phone_type = info['phone_type']
    return info

'''
获取IP
'''
def getIpInfo(ip):
    res = requests.get("http://ip.goldrock.cn/lookup?ip=%s"%(ip))
    return json.loads(res.text)['info']

'''
创建文件夹
'''
def mkdir(path):
    # 引入模块
    # 去除首位空格
    path=path.strip()
    # 去除尾部 \ 符号
    path=path.rstrip("\\")
 
    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)
 
    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        # 创建目录操作函数
        os.makedirs(path) 
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        logger.warning(path+' 目录已存在')
        return False

'''
推送极光ID列表
'''
def pushRegistrationIDList(registrationIDList):
    push = _jpush.create_push()
    push.audience = jpush.all_
    push.notification = jpush.notification(alert="!hello python jpush api")
    push.platform = jpush.all_
    try:
        response=push.send()
    except common.Unauthorized:
        raise common.Unauthorized("Unauthorized")
    except common.APIConnectionException:
        raise common.APIConnectionException("conn")
    except common.JPushFailure:
        logger.error("JPushFailure")
    except:
        logger.error("Exception")

'''
function:压缩
params:
    file_path:要压缩的件路径,可以是文件夹
    zfile_path:解压缩路径
description:可以在python2执行
'''
def zip_dir(file_path,zfile_path):
    filelist = []
    if os.path.isfile(file_path):
        filelist.append(file_path)
    else :
        for root, dirs, files in os.walk(file_path):
            for name in files:
                filelist.append(os.path.join(root, name))

    zf = zipfile.ZipFile(zfile_path, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar[len(file_path):]
        zf.write(tar,arcname)
    zf.close()

'''
    function:解压
    params:
        zfile_path:压缩文件路径
        unzip_dir:解压缩路径
    description:
'''
def unzip_file(zfile_path, unzip_dir):
    try:
        with zipfile.ZipFile(zfile_path) as zfile:
            zfile.extractall(path=unzip_dir)
    except zipfile.BadZipFile as e:
        print (zfile_path+" is a bad zip file ,please check!")

# dividend / divider
def divide(dividend, divider):
    dividend_number = float(dividend)
    divider_number = float(divider)
    if divider_number == 0:
        # logger.warning('0不可为除数')
        return 0
    else:
        return dividend_number/divider_number


def main(argv):
    # conn = getConnection({'db': 'mongo', 'host': '0.0.0.0', 'port': 27017, 'user': 'bi', 'password': '123456', 'database': 'bi'})
    # db = getMongoDb(conn)
    # collection = db['test']
    # dataItem = {'date':'2019-06-10', 'stats': [{'a':11, 'b':22}]}
    # print(collection.update({'date': dataItem['date']},{'$set': {'stats': dataItem['stats']}}, upsert=True))
    pass

if __name__ == "__main__":
    main(sys.argv[1:])
