# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 15:20:41 2017

@author: ligong

@description:这是布隆过滤的服务
"""
import json
import uwsgi
import time
import traceback
import urlparse
import util
import redis
import pymongo
import sys
from bloom_filter_plus import bloom_filter_plus

reload(sys)
sys.setdefaultencoding('utf-8')

config_path = 'config.json'
config = json.load(open(config_path))

BLOOMFILTER = None

                
def init_service():
    global config,RELOAD_HISTORY_THREADS,WRITE_HISTORY_THREADS,BLOOMFILTER
    mongo_url,redis_url = config['mongo_url'],config['redis_url']
    mongo_conn = pymongo.MongoClient(mongo_url).bloomfilter.history
    redis_conn = redis.StrictRedis.from_url(redis_url)
    
    BLOOMFILTER = bloom_filter_plus(redis_url,mongo_url)
    

uwsgi.post_fork_hook = init_service

def default_handler(env):
    query = env.get("QUERY_STRING", "")
    param = {k: v for (k, v) in urlparse.parse_qsl(query)}
    
    try:
        query_id,dest_id =  param['query_id'],param['dest_id']
        if param['action'] == 'add':
            BLOOMFILTER.add(query_id,dest_id)
            return {'success':True,'message':'OK!','code':200}
        elif param['action'] == 'query':
            if BLOOMFILTER.contains(query_id,dest_id):
                return {'success':True,'message':'OK','code':200,'data':{'exists':True}}
            return {'success':True,'message':'OK','code':200,'data':{'exists':False}}
    except:
        traceback.print_exc()
        return {'success':False,'message':'Param Error!','code':404}


def application(env, start_response):
    header = [('Content-Type', 'application/json;charset=utf8')]
    End = {'success':False,'message':'ERROR!','code':404}
    try:
        End = default_handler(env)
        start_response('200 OK', header)
    except Exception:
        traceback.print_exc()
        start_response('200 OK', [])
    
    return json.dumps(End,ensure_ascii=False).encode('utf8','ignore')

