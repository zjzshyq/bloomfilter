# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 13:42:21 2017

@author: ligong

@description:这是处理数据重载的程序
"""
import json
import time
import traceback

def murmurhash(data, seed=0):
    """
    data:需要hash的数据
    """
    c1 = 0xcc9e2d51 
    c2 = 0x1b873593 
    r1 = 15 
    r2 = 13 
    m = 5 
    n = 0xe6546b64
    length = len(data)
    h1 = seed
    rounded_end = (length & 0xfffffffc)
    for i in range(0, rounded_end, 4):
        k1 = (ord(data[i]) & 0xff) | ((ord(data[i + 1]) & 0xff) << 8) | \
             ((ord(data[i + 2]) & 0xff) << 16) | (ord(data[i + 3]) << 24)
        k1 *= c1
        k1 = (k1 << r1) | ((k1 & 0xffffffff) >> (32-r1))
        k1 *= c2
    
        h1 ^= k1
        h1 = (h1 << r2) | ((h1 & 0xffffffff) >> (32-r2))
        h1 = h1 * m + n
    k1 = 0
    
    val = length & 0x03
    if val == 3:
        k1 = (ord(data[rounded_end + 2]) & 0xff) << 16
    if val in [2, 3]:
        k1 |= (ord(data[rounded_end + 1]) & 0xff) << 8
    if val in [1, 2, 3]:
        k1 |= ord(data[rounded_end]) & 0xff
        k1 *= c1
        k1 = (k1 << r1) | ((k1 & 0xffffffff) >> (32-r1))
        k1 *= c2
        h1 ^= k1
    
    h1 ^= length
    h1 ^= ((h1 & 0xffffffff) >> 16)
    h1 *= 0x85ebca6b
    h1 ^= ((h1 & 0xffffffff) >> 13)
    h1 *= 0xc2b2ae35
    h1 ^= ((h1 & 0xffffffff) >> 16)
    return h1 & 0xffffffff

    
def hash_values(string,k,m):
    """
    获得多个hash值
    """
    return [murmurhash(string,i)  % m  for i in xrange(k)]

def delete_redis_key(redis_conn,remove_bit_key,remove_hash_key):
    """
    删除key
    remove_bit_key:删除bitmap
    remove_hash_key:删除hash key
    """
    redis_conn.delete(remove_bit_key)
    redis_conn.hdel(remove_hash_key,'second')
    
def reload_history_data(mongo_conn,redis_conn,hash_funcs,bit_len,query_id,timestamp,des_key):
    """
    把历史记录重新加载到bitmap中去
    mongo_conn:mongo链接
    redis_conn:redis链接
    hash_funcs:hash函数的个数
    bit_len:bit位的长度
    query_id:查询的主键
    timestamp:时间戳
    des_key:目标的键
    """
    for item in mongo_conn.find({'query_id':query_id,'time':{'$lte':timestamp}},{'dest_id':1}):
        try:
            values = hash_values(item['dest_id'],hash_funcs,bit_len)
            for v in values:
                redis_conn.setbit(des_key,v,1)
        except:
            traceback.print_exc()
            
def insert_data_into_mongo(mongo_conn,redis_conn,msg_queue):
    """
    把数据从redis的消息队列中取出，然后写到mongo中去
    mongo_conn:mongo链接
    redis_conn:redis链接
    msg_queue:消息队列的名字
    """
    while True:
        try:
            data = redis_conn.lpop(msg_queue)
            if data is None:
                time.sleep(1)
                continue
            mongo_conn.save(json.loads(data))
        except:
            traceback.print_exc()


    
    

