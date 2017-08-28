# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 14:09:33 2017

@author: ligong

@description:布隆过滤
"""
import redis
import pymongo
import util
import math
import time
import json

class bloom_filter_plus(object):
    def __init__(self,redis_url,mongo_url,n=5000,delta=0.01,expire_time = 86400*60):
        """
        redis_url:缓存地址
        mongo_url：mongo地址，用于记录
        n:初始的量
        delta:错误率
        expire_time:过期时间
        """
        self.redis_url = redis_url
        self.mongo_url = mongo_url
        self.n = n
        self.delta = delta
        self.expire_time = expire_time
        
        #获得需要的bit位m和hash函数的个数k
        self.m = int(math.log(1/delta)*n/(math.log(2)**2))+1
        self.k = int(self.m*math.log(2)/n)+1

        #用于存放添加到布隆过滤器数据的队列名字
        self.msg_queue_name = 'bloom_filter_msgq'
        
        #用重新加载的队列名
        self.reload_queue_name = 'bloom_filter_reload'
        
        #记录放到布隆过滤器中的历史数据，要用
        self.mongo_conn = pymongo.MongoClient(mongo_url).bloomfilter.history

        #键值信息的前缀
        self.key_info_prefix = 'INFO_%s'
        
        #缓存信息的reids
        self.cache = redis.StrictRedis.from_url(redis_url)
        
        
    def contains(self,key,string):
        """
        判断有没有出现
        """
        info_key = self.key_info_prefix % key
        info_data = self.cache.hgetall(info_key) or {}
        first_key,second_key  = info_data.get('first'),info_data.get('second')
        if first_key is None:
            return False
        #获得hash值
        values = util.hash_values(string,self.k,self.m)

        pipe = self.cache.pipeline()
        for h in values:
            pipe.getbit(first_key,h)
        
        result = (sum(pipe.execute()) == self.k)
        if result or second_key is None:
            return result

        values = util.hash_values(string,self.k,self.m >> 1)
        for h in values:
            pipe.getbit(first_key,h)
        return sum(pipe.execute()) == self.k
        
    def add(self,key,string):
        """
        添加到过滤器中去
        first:第一个键
        second:第二个键
        total:目前的数量
        support:支持的数量
        key:原本key
        """
        if self.contains(key,string):
            #print 'Already in!'
            return
        info_key = self.key_info_prefix % key
        info_data = self.cache.hgetall(info_key) or {}
        create_time = float(info_data.get('time',time.time()))
        ttl = int(self.expire_time - time.time() + create_time) or self.expire_time

        total = int(info_data.get('total',0))
        support = int(info_data.get('support',self.n))
        first_key,second_key  = info_data.get('first','%s_1' % key),info_data.get('second')
        
        #print first_key,second_key,ttl,total,support 
        pipe = self.cache.pipeline()
        if total >= support:
            print 'Expend'
            #这里要进行扩容
            second_key = '%s_2' % key
            support = support*2
            self.m = self.m * 2
            first_key,second_key = second_key,first_key
            data = {'query_id':key,'hash_funcs':self.k,'bit_len':self.m,'timestamp':time.time(),
                    'des_key':first_key,'remove_bit_key':second_key,'remove_hash_key':info_key}
            pipe.rpush(self.reload_queue_name,json.dumps(data))

        values = util.hash_values(string,self.k,self.m)
        #添加到redis
        
        for v in values:
            pipe.setbit(first_key,v,1)
        
        total += 1
        #更新信息
        pipe.hset(info_key,'total',total)
        pipe.hset(info_key,'support',support)
        pipe.hset(info_key,'first',first_key)
        pipe.hset(info_key,'key',key)
        pipe.hset(info_key,'time',create_time)
        
        #设置ttl
        pipe.expire(info_key,ttl)
        pipe.expire(first_key,ttl)
        if second_key:
            pipe.hset(info_key,'second',second_key)
            pipe.expire(second_key,ttl)
        
        #添加到历史记录中去
        data = {'query_id':key,'time':time.time(),'dest_id':string}
        pipe.rpush(self.msg_queue_name,json.dumps(data))
        pipe.execute()
        
        
        
        
        
