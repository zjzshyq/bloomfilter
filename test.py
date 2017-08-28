# -*- coding: utf-8 -*-
"""
Created on Mon Aug 28 09:29:18 2017

@author: ligong

@description:这是测试
"""
import requests
import time
a = time.time()
for i in range(10000):
    p = {'query_id':'twtwetwetw','dest_id':str(i),'action':'query'}
    requests.get('http://127.0.0.1:11111',p)
print (time.time()-a)/5100
        

