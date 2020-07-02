#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 23:11:34 2020

@author: ddd
"""
import enum
import functools as ft
import pandas as pd
import numpy as np
from collections.abc import Mapping,Sequence,AsyncIterable,Iterable,Iterator,AsyncIterator

from datetime import datetime
from binarctic.utils import chunked, achunked
# from .chunker import 
# @classmethod
# def fromtimestamp_ms(cls,ms):
#     return cls.utcfromtimestamp(ms/1000)
# pd.Timestamp.fromtimestamp_ms = fromtimestamp_ms
# fromtimestamp_ms = pd.Timestamp.fromtimestamp_ms

# def timestamp_ms(ts):
#     return int(1000 * ts.timestamp())
# pd.Timestamp.timestamp_ms=timestamp_ms


class KInterval(str,enum.Enum):
    KLINE_INTERVAL_1MINUTE = '1m'
    KLINE_INTERVAL_3MINUTE = '3m'
    KLINE_INTERVAL_5MINUTE = '5m'
    KLINE_INTERVAL_15MINUTE = '15m'
    KLINE_INTERVAL_30MINUTE = '30m'
    KLINE_INTERVAL_1HOUR = '1h'
    KLINE_INTERVAL_2HOUR = '2h'
    KLINE_INTERVAL_4HOUR = '4h'
    KLINE_INTERVAL_6HOUR = '6h'
    KLINE_INTERVAL_8HOUR = '8h'
    KLINE_INTERVAL_12HOUR = '12h'
    KLINE_INTERVAL_1DAY = '1d'
    KLINE_INTERVAL_3DAY = '3d'
    KLINE_INTERVAL_1WEEK = '1w'
    # KLINE_INTERVAL_1MONTH = '1M'

    # def freq(self):
    #     return self.replace('m','t')

    __str__=lambda self: str.__str__(self)

    def timedelta(self):
        return pd.Timedelta(int(self[:-1]),unit=self[-1])
    
    def to_milliseconds(self):
        return 1000*int(self.timedelta().total_seconds())
    
    # def chunker(self):
    #     if self.timedelta()<=self.KLINE_INTERVAL_5MINUTE.timedelta():
    #         return chunker.day_chunker
    #     if self.timedelta()<=self.KLINE_INTERVAL_2HOUR.timedelta():
    #         return chunker.month_chunker
    #     return chunker.year_chunker

KLRec=np.dtype([('date','M8[ms]'),
                ('o',np.float),
                ('h',np.float),
                ('l',np.float),
                ('c',np.float),
                ('v',np.float),
                ])

def bin_to_rec(b):
    return (pd.Timestamp.utcfromtimestamp(b[0]/1000),*b[1:6])

def bin_to_recarray(b):
    recs=[*map(bin_to_rec,b)]
    return np.rec.array(recs,dtype=KLRec)

def bin_to_df(b):
    df=pd.DataFrame(bin_to_recarray(b)).set_index('date')
    return df

class KLIterator(Iterator,AsyncIterator):
    _apply=staticmethod(lambda kk:kk)
    
    def __init_subclass__(cls,apply=None):
        apply=apply or cls._apply
        cls._apply=staticmethod(apply)
    
    def __init__(self, symbol, interval, startTime=0, **kwargs):
        self.kwargs = {'limit':1000,**kwargs,'symbol':symbol,'interval':KInterval(interval)}
        self.startTime = startTime
        
    @property
    def sess(self):
        if not '_sess' in self.__dict__:
            from binarctic.binance import Session
            self._sess=Session()
        return self._sess
    @property
    def asess(self):
        if not '_asess' in self.__dict__:
            from binarctic.binance import ASession
            self._asess=ASession()
        return self._asess        
    
    symbol=property(lambda self:self.kwargs['symbol'])
    interval=property(lambda self:self.kwargs['interval'])
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
            
    def __next__(self):
        # from binarctic.binance import Session
        # kk = self.sess.klines(**self.kwargs)
        if kk:= self.sess.klines(**self.kwargs):
            self.startTime=kk[-1][6]
            return self._apply(kk)
        else:
            raise StopIteration

    async def __anext__(self):
        
        if kk:= await self.asess.klines(**self.kwargs):
            self.startTime=kk[-1][6]
            return self._apply(kk)
        else:
            raise StopAsyncIteration
            
            
class KLIterator2(KLIterator,apply=bin_to_df):
    pass






