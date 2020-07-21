#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 12:09:01 2020

@author: ddd
"""


# from binarctic.binance import Session,ASession
# import pandas as pd
# from binarctic.binance.klines import KLIterator2


from tests import test_libs
    
from binarctic.arctic import Arctic,DateRange,libwrapper
from binarctic.binance import Session,ASession,aggTrades


a=Arctic()
sym='BTCUSDT'

s=Session()
# test_libs.test_all(True)

i=s.klines_iterator(sym,'1m')
i=a.klines_1h.binance_iterator(sym)

# next(i)
# class Iterator:
#     def __init__(self,end_point,**kwargs):
#         self.end_point=end_point
#         self.kwargs=kwargs
        
#     def __next__(self):
#         data = self.end_point(**self.kwargs)
#         if data:
#             self._prepare(data)
#             return data
#         else:
#             raise StopIteration
        
#     async def __anext__(self):
#         data = await self.end_point(**self.kwargs)
#         if data:
#             self._prepare(data)
#             return data
#         else:
#             raise StopIteration       
        
#     def _prepare(self,data):
#         self.kwargs['startTime']=data[-1][6]
    
        

# Arctic.ticks=libwrapper.TickStore.factory('ticks')
# Arctic.ticks_w=libwrapper.TickStore_Wrapper.factory('ticks_w')

# del a.ticks
# del a.ticks_w


# del a.exchangeInfo
# a.exchangeInfo

# ei=a.exchangeInfo.ex_info


# a.exchangeInfo.get_symbol('ACC_INFO').read_history()
# a.exchangeInfo
