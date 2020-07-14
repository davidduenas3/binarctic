#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 09:40:15 2020

@author: ddd
"""

import pandas as pd
import pytz
from asyncio import create_task
from collections.abc import Mapping

from .arctic import LibFactory,Arctic,VersionStore_Wrapper,MetadataStore_Wrapper
from .arctic.libs import KeyProperty
from .arctic.libwrapper import wrapped_method,wrapped_attribute
from .binance import Session,ASession,rest_api
from .pipe import apply_fn

# rest_api.Reqs.ex_info=rest_api.Reqs.exchangeInfo.pipe

# exchangeInfo=rest_api.End_Point('v3/exchangeInfo', 'get')

# @exchangeInfo.pipe
# def exchangeInfo(ret):
#     ret['symbols'] = {i['symbol']:Symbol(i) for i in ret['symbols']}
#     return ret

class ExchangeInfo(Mapping):
    
    __keys = 'timezone', 'serverTime', 'rateLimits', 'exchangeFilters', 'symbols'
    
    __iter__ = lambda s : iter(s.__keys)
    __len__ = lambda s : len(s.__keys)
    keys = lambda s : s.__keys
    
    def __getitem__(self,name):
        return self._map[name]
    
    def __init__(self,*args,**kwargs):
        self._map = map = dict(*args,**kwargs)
        assert set(self) == set(map)
        
    # @classmethod
    # def from_request(cls,map):
    #     return cls(
    #         timezone = map.pop('timezone'),
    #         serverTime = map.pop('serverTime'),
    #         rateLimits = map.pop('rateLimits'),
    #         exchangeFilters = map.pop('exchangeFilters'),
    #         symbols = {i['symbol']:i for i in  map.pop('symbols')})
    
    @property
    def timezone(self):
        return pytz.timezone(self['timezone'])
    
    @property
    def serverTime(self):
        return pd.Timestamp(self['serverTime'],unit='ms',tz=self['timezone'])
    
    @property
    def rateLimits(self): 
        return self['rateLimits']
    
    @property
    def exchangeFilters(self):    
        return self['exchangeFilters']
    
    @property
    def symbols(self):
        return self['symbols']
    

rest_api.Reqs.exchangeInfo =(
    rest_api.End_Point('v3/exchangeInfo', 'get').pipe(ExchangeInfo))
                             

class Symbol(Mapping):
    def __init__(self,*args,**kwargs):
        self._map=dict(*args,**kwargs)
        self.__dict__.update(self._map)
        
    __iter__=property(lambda s:s._map.__iter__)
    __len__=property(lambda s:s._map.__len__)
    keys=property(lambda s:s._map.keys)
    
    def __getitem__(self,name):
        return self._map[name]

    def __str__(self):
        return self.symbol





@LibFactory.register_type("VS_ExchangeInfo")
class LibExchangeInfo(MetadataStore_Wrapper):
    
    append = wrapped_method()
    read = wrapped_method()
    
    def __init__(self):
        self.req_task=create_task(self.arequery())
    
    # @KeyProperty.Default(VersionStore_Wrapper.metadata,"EX_INFO")
    # def ex_info(self):
    #     sess=Session()
    #     return sess.exchangeInfo()
    
    @property
    def ex_info(self):
        return self.get_symbol('EX_INFO')
        
    
    @property
    def symbols(self):
        return self.ex_info.symbols


    
    async def arequery(self):
        self.ex_info.append(await ASession().exchangeInfo())
        print('\nareq')
        
        
@LibExchangeInfo.register_symbol('EX_INFO')
class ExInfoSymbol(object):
    
    read = apply_fn(ExchangeInfo)(LibExchangeInfo.Symbol.read)

# class ExInfoSymbol(LibExchangeInfo.Symbol):
    


Arctic.exchangeInfo=LibExchangeInfo.factory("exchange_info",)