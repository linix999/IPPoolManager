# -*- coding: utf-8 -*-
# @Time    : 11/01/19 11:15 PM
# @Author  : linix

import json
import datetime
import time
import re
import requests
import redis
from celery.task import task,periodic_task
from celery.schedules import crontab
from django.db.models import Max
from django.conf import settings
from hangzhou.models import ProxyDomain,ProxySet

proxyPoolServer=redis.Redis(host=settings.IP_POOL_REDIS_HOST,port=settings.IP_POOL_REDIS_PORT,password=settings.IP_POOL_REDIS_PWD)
domainZSetKey='PROXYPOOL:DOMAINS'
domainsProxyZSetKey='%(domain)s:ProxySortedSet'
proxyGetTimeZSetKey='PROXY:GETTIMESTAMP'
proxyGetFromHZSetKey='PROXY:GetFromHZSet'
oneHourLimitProxyNum=200

def proxyCheck():
    """
    :return: 先清理超时代理，再循环每个域名，获取代理池设置值与可用数量最大相差值，将差额IP补上
    """
    batchProxyMaxNum=16
    getDatabaseProxy()
    cleanExpiresIP()
    domains=proxyPoolServer.zrange(domainZSetKey, 0, -1,withscores=True)
    maxDelta=0
    maxDeltaDomainName=None
    maxProxyNum=0
    maxProxyNumDomainName=None
    for domain in domains:
        poolSize=int(domain[1])
        proxyNums=proxyPoolServer.zcard(domainsProxyZSetKey %{'domain':str(domain[0],'utf-8').upper()})
        if poolSize-proxyNums>maxDelta:
            maxDelta=poolSize-proxyNums
            maxDeltaDomainName=str(domain[0],'utf-8').upper()
        if proxyNums>maxProxyNum:
            maxProxyNum=proxyNums
            maxProxyNumDomainName=str(domain[0],'utf-8').upper()
    if maxDelta>batchProxyMaxNum:
        maxDelta=batchProxyMaxNum
    if maxDeltaDomainName and proxyPoolServer.exists(domainsProxyZSetKey %{'domain':maxDeltaDomainName}):
        if not limitGetProxyNums():
            getNewIP(ipNum=maxDelta)
    elif maxProxyNumDomainName and maxDeltaDomainName:  #新建的域名先将最多IP的集合并过来
        proxyPoolServer.zunionstore(domainsProxyZSetKey % {'domain': maxDeltaDomainName}, (domainsProxyZSetKey % {'domain': maxDeltaDomainName}, domainsProxyZSetKey % {'domain': maxProxyNumDomainName}))

def cleanExpiresIP(timeDelta=5):
    """
    清理每个域名下超时代理(timeDelta为提前量)，如果清理后所有代理池为空，则请求一条长时IP保存在里面
    :return:
    """
    domains = proxyPoolServer.zrange(domainZSetKey, 0, -1)
    nowTime=int(time.time())
    totalProxyNum=0
    for domain in domains:
        proxySet=proxyPoolServer.zrange(domainsProxyZSetKey %{'domain':str(domain,'utf-8').upper()},0,-1,withscores=True)
        for proxy in proxySet:
            if int(proxy[1])<nowTime+timeDelta:
                proxyPoolServer.zrem(domainsProxyZSetKey %{'domain':str(domain,'utf-8').upper()},str(proxy[0],'utf-8'))
        totalProxyNum+=proxyPoolServer.zcard(domainsProxyZSetKey %{'domain':str(domain,'utf-8').upper()})

    if totalProxyNum==0 and domains:
        getNewIP(timeType=4,ipNum=1)

def getNewIP(timeType=1,ipNum=1):
    """
    :param self:
    :param timeType:
    :param ipNum:
    :return: 获取IP并插入每个域名中
    """
    print("timeType is :%s,ipNum is :%s" %(timeType,ipNum))
    if ipNum<1 or timeType<1 or timeType>4:
        return
    domains = proxyPoolServer.zrange(domainZSetKey,0,-1)
    # timeType为1:5 - 25分钟：0.04元/IP ;2:25分钟 - 3小时：0.10元/IP;3:3 - 6小时：0.20元/IP;4:6 - 12小时：0.50元/IP
    urlPatt = "xxx"
    reqUrl=urlPatt.format(num=str(ipNum),timeType=str(timeType))
    with requests.Session() as s:
        s.keep_alive = False
        res = requests.get(reqUrl, verify=True,timeout=5)
        try:
            info = json.loads(res.text)
            codeType=info.get('code')
        except BaseException as e:
            print("解析代理网站json出错，reason:%s" %e)
            return
    if codeType==0:
        proxyList=info.get('data',[])
        for proxy in proxyList:
            proxyIp=proxy.get('ip')+':'+str(proxy.get('port'))
            expireTime=proxy.get('expire_time')
            timeArray=datetime.datetime.strptime(expireTime, "%Y-%m-%d %H:%M:%S")
            timestamp=int(time.mktime(timeArray.timetuple()))
            for domain in domains:
                proxyPoolServer.zadd(domainsProxyZSetKey %{'domain':str(domain,'utf-8').upper()},{proxyIp:timestamp})
            nowTimestamp=int(time.time())
            proxyPoolServer.zadd(proxyGetTimeZSetKey,{proxyIp:nowTimestamp})
        saveProxy2Hangzhou(proxyList)
    elif codeType==111:
        print("稍后再试")
    elif codeType==113 or codeType==117:
        print("准备添加白名单")
        ip = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', info.get('msg'))[0]
        url = "http://xxx" % ip
        res = requests.get(url)
    elif codeType==114:
        print("代理IP余额不足,请充值")
    elif codeType==121:
        print("今日套餐已用完")

def resetPoolSize(minPoolSize=1):
    """
    :param minPoolSize: 
    :return: 每隔一长段时间将每个IP池代理重置为1条，防止没有爬虫运行时维护代理池的开支
    """
    domains = proxyPoolServer.zrange(domainZSetKey, 0, -1)
    for domain in domains:
        proxyPoolServer.zadd(domainZSetKey,{str(domain,'utf-8').upper():minPoolSize})

def cleanNotUsedProxyZSet():
    """
    :return: 每隔一长时间将不在域名集合中的代理集合删除
    """
    domains = proxyPoolServer.zrange(domainZSetKey, 0, -1)
    proxySets=proxyPoolServer.keys('*:ProxySortedSet')
    for proxySet in proxySets:
        domainName=str(proxySet,'utf-8').rstrip(':ProxySortedSet')
        if not domainName.encode('utf-8') in domains:
            proxyPoolServer.delete(proxySet)

def cleanNotUsedProxyGetTimeZSet():
    """
    清除超过2个小时的代理获取时间集合数据
    """
    nowTimestamp = int(time.time())
    proxyPoolServer.zrangebyscore(proxyGetTimeZSetKey,0,nowTimestamp-7200)

def cleanGetFromHZProxySet():
    """
    每隔一段时间清除保存的从杭州获取的代理集合
    """
    proxyPoolServer.delete(proxyGetFromHZSetKey)

def limitGetProxyNums():
    """
    限制每小时获取代理的条数，功能比较鸡肋。值为请求时间，获取请求时间段的集合数量。
    """
    nowTimestamp = int(time.time())
    getProxyNum=proxyPoolServer.zcount(proxyGetTimeZSetKey,nowTimestamp-3600,nowTimestamp)
    if getProxyNum>=oneHourLimitProxyNum:
        return True
    return False

def getDatabaseProxy(timeDelta=10):
    """
    从杭州数据库取IP存入到Redis中去
    """
    t=datetime.datetime.now()+datetime.timedelta(seconds=timeDelta)
    redisDomains = proxyPoolServer.zrange(domainZSetKey, 0, -1)
    try:
        proxyObjs = ProxySet.objects.values('ip').filter(expireTime__gt=t).annotate(expireTime=Max('expireTime'))
        for proxy in proxyObjs:
            if proxy.get('ip') and proxy.get('expireTime'):
                if not proxyPoolServer.sismember(proxyGetFromHZSetKey,proxy.get('ip')):
                    print("从杭州数据库获取IP：%s插入Redis中" % proxy.get('ip'))
                    proxyPoolServer.sadd(proxyGetFromHZSetKey,proxy.get('ip'))
                    timestamp = int(time.mktime(proxy.get('expireTime').timetuple()))
                    for domain in redisDomains:
                        proxyPoolServer.zadd(domainsProxyZSetKey % {'domain': str(domain, 'utf-8').upper()}, {proxy.get('ip'): timestamp})
    except BaseException as e:
        print("数据库又断了...")

def saveProxy2Hangzhou(proxyList):
    """
    将自己请求的代理存入到杭州数据库中
    """
    mysqlDomains=ProxyDomain.objects.all()
    for proxy in proxyList:
        proxyIp = proxy.get('ip') + ':' + str(proxy.get('port'))
        expireTime = proxy.get('expire_time')
        print("存入代理：%s" %proxyIp)
        for domain in mysqlDomains:
            try:
                ip=ProxySet(ip=proxyIp,expireTime=expireTime,domain=domain.domain)
                ip.save()
            except BaseException as e:
                print("存入报错，原因为：%s" % e)

@periodic_task(run_every=3)
def maintenanceProxyPoolTask(**kwargs):
    proxyCheck()
    return True

@periodic_task(run_every=30*60)
def resetProxyPoolSizeTask(**kwargs):
    resetPoolSize()
    return True

@periodic_task(run_every=2*60*60)
def cleanRedisDataTask(**kwargs):
    cleanNotUsedProxyZSet()
    cleanNotUsedProxyGetTimeZSet()
    cleanGetFromHZProxySet()
    return True