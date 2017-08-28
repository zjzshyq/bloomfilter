# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 09:53:34 2017

@author: ligong

@description:这是bloom filter的service的后台服务
"""
import json
import time
import threading
import traceback
import util
import pymongo
import redis

class reload_history(threading.Thread):
    def __init__(self,thread_name,redis_conn,mongo_conn,qname):
        '''
        @summary: 初始化对象
        mongo_conn:mongo地址
        redis_conn:缓存链接
        qname:队列名
        '''
        super(reload_history, self).__init__(name = thread_name)
        self.mongo_conn = mongo_conn
        self.redis_conn = redis_conn
        self.qname = qname
        self.daemon = True
        #self.start()
        
    def run(self):
        '''
        @summary: 重写父类run方法
        '''
        while True:
            try:
                data = self.redis_conn.lpop(self.qname)
                if data is None:
                    time.sleep(1)
                    continue
                t_dict = json.loads(data)
                print t_dict
                hash_funcs = t_dict['hash_funcs']
                bit_len = t_dict['bit_len']
                query_id = t_dict['query_id']
                timestamp = t_dict['timestamp']
                des_key = t_dict['des_key']
                remove_bit_key = t_dict['remove_bit_key']
                remove_hash_key = t_dict['remove_hash_key']
                #写到mongo
                util.reload_history_data(self.mongo_conn,self.redis_conn,hash_funcs,bit_len,query_id,timestamp,des_key)
                #删除redis中的无效信息
                util.delete_redis_key(self.redis_conn,remove_bit_key,remove_hash_key)
            except:
                traceback.print_exc()
        
        
class write_history(threading.Thread):
    def __init__(self,thread_name,redis_conn,mongo_conn,qname):
        '''
        @summary: 初始化对象
        mongo_conn:mongo地址
        redis_conn:缓存链接
        qname:队列名
        '''
        super(write_history, self).__init__(name = thread_name)
        self.mongo_conn = mongo_conn
        self.redis_conn = redis_conn
        self.qname = qname
        self.daemon = True
        #self.start()
        
    def run(self):
        '''
        @summary: 重写父类run方法
        '''
        util.insert_data_into_mongo(self.mongo_conn,self.redis_conn,self.qname)
        
if __name__ == '__main__':
    config_path = 'config.json'
    config = json.load(open(config_path))

    #存放各个线程
    RELOAD_HISTORY_THREADS = []
    WRITE_HISTORY_THREADS = []
    mongo_url,redis_url = config['mongo_url'],config['redis_url']
    mongo_conn = pymongo.MongoClient(mongo_url).bloomfilter.history
    redis_conn = redis.StrictRedis.from_url(redis_url)
    reload_history_threads,write_history_threads= config['reload_history_no'],config['write_history_no']
    
    #初始化各个线程
    for i in xrange(reload_history_threads):
        t = reload_history('reload_history_%s' % i,redis_conn,mongo_conn,'bloom_filter_reload')
        RELOAD_HISTORY_THREADS.append(t)
        t.start()
    
    for i in xrange(write_history_threads):
        t = write_history('write_history_%s' % i,redis_conn,mongo_conn,'bloom_filter_msgq')
        WRITE_HISTORY_THREADS.append(t)
        t.start()
    
    for t in RELOAD_HISTORY_THREADS:
        t.join()
    for t in WRITE_HISTORY_THREADS:
        t.join()
        

