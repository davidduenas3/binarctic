#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 16 10:04:12 2020

@author: ddd
"""


import enum
import functools as ft
import pandas as pd
import numpy as np
# from collections.abc import Mapping,Sequence,AsyncIterable,Iterable,Iterator,AsyncIterator

from datetime import datetime
from binarctic.utils import chunked, achunked
from binarctic.binance import rest_api

class AggTradesIterator(rest_api.Iterator):
    
    def __init__(self,symbol,*,startTime=None,fromId=0):
        super().__init__({'symbol':symbol})
        if startTime is None: 
            self.startTime=self._get_startTime(fromId)
        else:
            self.startTime=startTime

    startTime=rest_api.Iterator.startTime
    @startTime.setter
    def startTime(self,value):
        rest_api.Iterator.startTime.__set__(self,value)
        value=self.startTime
        self.kwargs['endTime']=value+1000*60*60
    
    def _get_startTime(self,id_):
        return self.sess.aggTrades(self.symbol,fromId=id_,limit=1)[0]['T']
        
    def __next__(self):

        if kk:= self.sess.aggTrades(**self.kwargs):
            self.startTime=kk[-1]['T']+1
            return kk
        else:
            raise StopIteration

    async def __anext__(self):
        
        if kk:= await self.sess.aggTrades(**self.kwargs):
            self.startTime=kk[-1]['T']+1
            return kk
        else:
            raise StopAsyncIteration