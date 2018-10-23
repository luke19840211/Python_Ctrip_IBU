# -*- coding: utf-8 -*-
'''
Created on 2018��10��19��

@author: haiyongli
'''

from pyspark import SparkContext
from pyspark import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
import pandas as pd
import datetime
from numpy import array
import urllib
import urllib2
import json
from time import time
import Queue

sc = SparkContext()
sqlContext = HiveContext(sc)
def quiet_logs( sc ):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
   
quiet_logs(sqlContext)

def ip_decode(line):
    url = 'http://ws.mdm.sh.ctripcorp.com/Arch-MDM-IPAddress-WS/api/json/getipinfo'
    # values = {'IP':'73.65.189.111'}
    q = Queue.Queue()
    data = urllib.urlencode(line[1])
    req = urllib2.Request(url, data)
    response = urllib2.urlopen(req)
    geo = response.read()
    geo = json.loads(geo)['Result']
    if geo.has_key('CountryCode'):
        lastlog_countrycode = geo['CountryCode']
    else:
        lastlog_countrycode = None
    if geo.has_key('CityCH'):
        lastlog_citych = geo['CityCH']
    else:
        lastlog_citych = None
    if geo.has_key('CountryNameCH'):
        lastlog_countryname = geo['CountryNameCH']
    else:
        lastlog_countryname = None
    if geo.has_key('ProvinceCH'):
        lastlog_province = geo['ProvinceCH']
    else:
        lastlog_province = None
    return line[0],lastlog_countrycode,lastlog_citych,lastlog_countryname,lastlog_province

def trans_line(line):
    line = list(line)
    line[1] = {'IP': line[1]}
    return tuple(line)
    
#sql_query = "select uid,lastloginip from dw_engdb.bbzdb_engmembers where d = '2018-04-19'"
sql_query = "select uid,lastloginip from tmp_engdb.tmp_parse_ip_region_lhy where lastloginip <>''"
raw_data = sqlContext.sql(sql_query)
raw_data1 = raw_data.map(lambda line: trans_line(line))
raw_data2 = raw_data1.map(lambda line: ip_decode(line))
raw_data3 = raw_data2.map(lambda p: Row(uid=p[0],lastlog_countrycode=p[1],lastlog_citych=p[2],lastlog_countryname=p[3],lastlog_province=p[4]))

df = sqlContext.createDataFrame(raw_data3)
print df.head()
df.registerTempTable('tmpTableip')
sqlContext.sql("use tmp_engdb")
sqlContext.sql("drop table if exists tmp_parse_lastloginip_region_result_lhy")
sqlContext.sql("create table tmp_engdb.tmp_parse_lastloginip_region_result_lhy as select uid,lastlog_countrycode,lastlog_citych,lastlog_countryname,lastlog_province from tmpTableip")