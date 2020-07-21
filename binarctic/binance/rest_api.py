#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 19 11:02:42 2020

@author: ddd
"""


import aiohttp
import asyncio
import hashlib
import hmac
import requests
import time
import enum
import json
import functools as ft
import itertools as itt
import pandas as pd
from abc import  ABC, abstractmethod
from operator import itemgetter
# from binarctic.binance.exception import  BinanceAPIException, BinanceRequestException
from binarctic.binance import api_config
from binarctic.binance import models
from binarctic.pipe import Pipable, apply_fn
from binarctic import pipe
from binarctic.utils import *
# from .klines import KInterval

from inspect import (Signature,Parameter,signature,iscoroutine,
                     iscoroutinefunction,ismethoddescriptor,isfunction,
                     ismethod,isasyncgen,isgenerator,isasyncgenfunction,
                     isgeneratorfunction)
from types import  MethodType
from collections import namedtuple, abc
from datetime import datetime

__all__ = ['Session','ASession']


class BinanceAPIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res['code']
            self.message = json_res['msg']
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)    
    
class BinanceRequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'BinanceRequestException: %s' % self.message


# class KInterval(str,enum.Enum):
#     KLINE_INTERVAL_1MINUTE = '1m'
#     KLINE_INTERVAL_3MINUTE = '3m'
#     KLINE_INTERVAL_5MINUTE = '5m'
#     KLINE_INTERVAL_15MINUTE = '15m'
#     KLINE_INTERVAL_30MINUTE = '30m'
#     KLINE_INTERVAL_1HOUR = '1h'
#     KLINE_INTERVAL_2HOUR = '2h'
#     KLINE_INTERVAL_4HOUR = '4h'
#     KLINE_INTERVAL_6HOUR = '6h'
#     KLINE_INTERVAL_8HOUR = '8h'
#     KLINE_INTERVAL_12HOUR = '12h'
#     KLINE_INTERVAL_1DAY = '1d'
#     KLINE_INTERVAL_3DAY = '3d'
#     KLINE_INTERVAL_1WEEK = '1w'
#     KLINE_INTERVAL_1MONTH = '1M'

#     __str__=lambda self: str.__str__(self)

# class _KLIterable():
#     _code='''{}def __{}iter__(self):
#             buffer=None
#             while True:
#                 if buffer is None:
#                     if chunk:= {}self.req(startTime=self.last+1):
#                         buffer = iter(chunk)
#                     else:
#                         return
#                 try:
#                     ret=next(buffer)
#                     yield ret
#                     self.last=ret[0]  
#                 except StopIteration:
#                     buffer=None'''  
    
#     def __init__(self,sess,symbol,interval='1h',startTime=0):
#         self.req=ft.partial(sess.klines,symbol,KInterval(interval),limit=1000)
#         self.last=startTime-1         
    

    
class DataParam(Parameter):
    type=staticmethod(lambda x:x)
    
    
    def __init__(self, name, mandatory=False, *, type=None, default=Parameter.empty):
        super().__init__(name,
                         Parameter.POSITIONAL_OR_KEYWORD if mandatory else Parameter.KEYWORD_ONLY,
                         default = Parameter.empty if mandatory else default)
        self.type = type or self.type
    
    
    mandatory=classmethod(lambda cls,name,type,**kwargs:cls(name,mandatory=True,type=type,**kwargs))
    optional=classmethod(lambda cls,name,type,**kwargs:cls(name,mandatory=False,type=type,**kwargs))
    


class End_Point(Pipable):
    API_URL = 'https://api.binance.com/api'
    
    
    def __init__(self, path, method, signed=False, *, parameters=()):
        self.method=method
        self.uri='/'.join([self.API_URL,path])
        self.signed=signed
        self.parameters=parameters
        # self.__signature__=Signature(
        #         parameters=(Parameter('self',Parameter.POSITIONAL_ONLY),*parameters))
    @property
    def __signature__(self):
        parameters=(Parameter('self',Parameter.POSITIONAL_ONLY),*self.parameters)
        return Signature(parameters)
#                 
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        return MethodType(self,ins)
    
    def __call__(self,ins,*args,**kwargs):
    
        data = kwargs.pop('data',dict())
        args = list(args)
        
        for p in self.parameters:
            k, v= p.name, p.default
            if p.kind==Parameter.POSITIONAL_OR_KEYWORD:
                v = kwargs.pop(k) if k in kwargs else args.pop(0) if args else v
            elif p.kind==Parameter.KEYWORD_ONLY:
                v = kwargs.pop(k, v)
            if not v is Parameter.empty:
                data[k]=p.type(v)
        
        kwargs['data'] = data
        kwargs = self._get_request_kwargs(ins,**kwargs)
        ret = ins._request(self.method, self.uri, **kwargs)
        return ret

            

    def _get_request_kwargs(self,ins,**kwargs):
        
        def _order_params(data):        
            params = [(k, str(data[k])) for k in sorted(data.keys()) if k!='signature']
            if 'signature' in data:
                params.append(('signature', data['signature']))
            return params

        kwargs['timeout'] = 10
        data = kwargs.get('data', None)
        
        # merge requests params into kwargs
        if data and 'requests_params' in data:
            kwargs.update(data['requests_params'])
            del data['requests_params']
        
        if self.signed:
            data['timestamp'] = int(time.time() * 1000)
            ordered_data = _order_params(data)
            query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in ordered_data])
            m = hmac.new(ins.key.api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
            data['signature'] = m.hexdigest()
        
        if data:
            kwargs['data'] = _order_params(data)
        if data and self.method=='get':
            kwargs['params'] = kwargs['data']
            del(kwargs['data'])
        return kwargs    
        







class _Base_Session(ABC):

    key = api_config.Key('','')

    @abstractmethod
    def create_session(self):
        pass
    
    @abstractmethod
    def _request(self,end_point,data,**kwargs):
        pass
    
    def __init__(self, key_name = 'default'):
        self.key =api_config.get(key_name)
        
        self._headers={'Accept': 'application/json',
                       'User-Agent': 'binance/python',
                       'X-MBX-APIKEY': self.key.api_key}    
    

    used_weight={}
    
    @classmethod
    def process_weight(cls,response):
        headers=response.headers
        keys='x-mbx-used-weight-1m','x-mbx-used-weight'
        weights = {k:headers[k] for k in keys if k in headers.keys()}
        cls.used_weight.update(weights)



    

class Reqs(object): 
    
    '''
        END_POINTS
    ''' 
    ping = End_Point('v3/ping', 'get')
    
    # exchangeInfo = End_Point('v3/exchangeInfo', 'get') 
    
    aggTrades = End_Point('v3/aggTrades','get',parameters=(
                        DataParam.mandatory('symbol',str),
                        DataParam.optional('fromId',int),
                        DataParam.optional('startTime',int),
                        DataParam.optional('endTime',int),
                        DataParam.optional('limit',int))
                )
    
    klines = End_Point('v3/klines',
                       'get',parameters=(
                            DataParam.mandatory('symbol',str),
                            DataParam.mandatory('interval',str),
                            DataParam.optional('startTime',int),
                            DataParam.optional('endTime',int),
                            DataParam.optional('limit',int)),
                        )
    
    avgPrice = End_Point('v3/avgPrice', 'get',  parameters=(
                        DataParam.mandatory('symbol',str),))
    
    ticker_price = End_Point('v3/ticker/price', 'get', parameters=(
                        DataParam.optional('symbol',str),))

        
    ticker_bookTicker = End_Point('v3/ticker/bookTicker', 'get', parameters=(
                        DataParam.optional('symbol',str),))

    account = End_Point('v3/account','get', signed=True)
    

    
    get_first_ts = pipe.partialmethod(klines,interval='1h',startTime=0,limit=1)\
            .pipe(lambda res:res[0][0])
    
    
    
    class _Iterator(ABC):
        def __init__(self,end_point,**kwargs):
            self.end_point=end_point
            self.apply=kwargs.pop('apply',lambda x:x)
            self.kwargs=kwargs
            
        def __next__(self):
            data = self.end_point(**self.kwargs)
            if data:
                self._prepare(data)
                return self.apply(data)
            else:
                raise StopIteration
            
        async def __anext__(self):
            data = await self.end_point(**self.kwargs)
            if data:
                self._prepare(data)
                return self.apply(data)
            else:
                raise StopIteration       
        
        @abstractmethod    
        def _prepare(self,data):
            self.kwargs['startTime']=data[-1][6]    
        
        def __iter__(self):
            return self
        
        async def __aiter__(self):
            return self    
        
    
    class KIterator(_Iterator):
        def __init__(self,sess,symbol,interval,startTime=0,**kwargs):
            super().__init__(sess.klines,symbol=symbol,interval=interval,limit=1000,**kwargs)
            self.startTime=startTime
            
        def _prepare(self,data):
            self.startTime=data[-1][6]
             
        startTime=property(lambda self:self.kwargs['startTime'])
    
        @startTime.setter
        def startTime(self,value):
            if isinstance(value,int):
                self.kwargs['startTime']=value
            elif isinstance(value,datetime):
                self.startTime=int(value.timestamp()*1000)
            elif isinstance(value,str):
                self.startTime=pd.Timestamp(value)
            else:
                raise TypeError('%s type??' % type(value))     
 
    
     
    @property
    def klines_iterator(self):
        from .klines import bin_to_df
        return ft.partial(self.KIterator,self,apply=bin_to_df)
    
    


    
class Session(_Base_Session,Reqs):
    
            
    def create_session(self):
        sess = requests.sessions.Session()
        sess.headers = self._headers
        return sess
    
    def _request(self, method, uri, **kwargs):

        with self.create_session() as sess:
            with getattr(sess, method)(uri, **kwargs) as response:
                # breakpoint()
                if not str(response.status_code).startswith('2'):
                    raise BinanceAPIException(response, response.status_code, response.text)
                try:
                    self.process_weight(response)
                    return response.json()
                except ValueError:
                    raise BinanceRequestException('Invalid Response: %s' % response.text)
    
   

            
            
            
class ASession(_Base_Session,Reqs):
    
    
    def create_session(self):
        return aiohttp.ClientSession(headers=self._headers)
            

    async def _request(self, method, uri, **kwargs):
        async with self.create_session() as sess:
            async with getattr(sess, method)(uri, **kwargs) as response:
                # print(response.headers)
                if not str(response.status).startswith('2'):
                    raise BinanceAPIException(response, response.status, await response.text())
                try:
                    self.process_weight(response)
                    json = await response.json()
                    return json
                except ValueError:
                    txt = await response.text()
                    raise BinanceRequestException('Invalid Response: {}'.format(txt))
    


class Iterator(abc.Iterator,abc.AsyncIterator):
    def __init__(self,kwargs):
        self.sess=Session()
        self.asess=ASession()
        self.kwargs = dict(kwargs)
    
    symbol=property(lambda self:self.kwargs['symbol'])
    # interval=property(lambda self:self.kwargs['interval'])
    startTime=property(lambda self:self.kwargs['startTime'])
    
    @startTime.setter
    def startTime(self,value):
        if isinstance(value,int):
            self.kwargs['startTime']=value
        elif isinstance(value,datetime):
            self.startTime=int(value.timestamp()*1000)
        elif isinstance(value,str):
            self.startTime=pd.Timestamp(value)
        else:
            raise TypeError('%s type??' % type(value))


    def __iter__(self):
        return self
    
    async def __aiter__(self):
        return self
    
if __name__=='__main__':
    
    sym='BTCUSDT'
    
    sess=Session()
    asess=ASession()
    
   
    
    
"""
    await ac.account()
    await ac.exchangeInfo()
"""


