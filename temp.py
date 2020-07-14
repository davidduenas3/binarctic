#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 12:09:01 2020

@author: ddd
"""


# from binarctic.binance import Session,ASession
# import pandas as pd
from binarctic.binance.klines import KLIterator2


from tests import test_libs
    
from binarctic.arctic import Arctic,DateRange,libwrapper
from binarctic.binance import Session,ASession


a=Arctic()
sym='BTCUSDT'
# test_libs.test_all(True)


Arctic.ticks=libwrapper.TickStore.factory('ticks')
Arctic.ticks_w=libwrapper.TickStore_Wrapper.factory('ticks_w')

del a.ticks
del a.ticks_w


# del a.exchangeInfo
# a.exchangeInfo

ei=a.exchangeInfo.ex_info.read()

# def _on_create(lib):
#     async def _coro():
#         lib.custom_data.update(await ASession().exchangeInfo())
        
#     import asyncio
#     asyncio.create_task(_coro())
    
# Arctic.exchange_info=libwrapper.VersionStore.factory('exchange_info',on_create=_on_create)
# sess=Session()



# async def update(symbol):
    
#     # del a.klines_1m
#     lib=a.klines_1m

#     async def binance_update():
#         async with lib.get_lock(symbol):
#             it=KLIterator2(symbol,lib.interval,lib.startTime(symbol))
#             async for df in it:
#                 lib.update(symbol,df,chunk_range=df.index,upsert=True)
#                 print(it.startTime)
                
#     while True:
#         await binance_update()
#         await asyncio.sleep(1)

    
    
# # task=asyncio.create_task(update(sym))

# class KLineUpdater(object):
#     @classmethod
#     def assert_symbols(cls,lib,symbols):
#         async def assert_sym(symbol):
#             if lib._get_symbol_info(symbol) is None:
#                 it=KLIterator2(symbol,lib.interval,0,limit=100)
#                 df=await it.__anext__()
#                 lib.write(symbol,df)
#         coros = map(assert_sym,symbols)
#         return asyncio.gather(*coros)
    
#     def __init__(self,lib,symbol):
#         self.lib=lib
#         self.symbol=symbol
#         if self.lib._get_symbol_info(symbol) is None:
#             raise KeyError("%s not found" % symbol)
#         self.df=self.read()
#         self.last=self.df.index[-1]
        
#     def read(self,limit=100000):
#         data=pd.DataFrame()
#         for df in self.lib.reverse_iterator(self.symbol):
#             data=pd.concat([df,data])
#             if len(data)>limit:
#                 data=data[-limit:]
#                 break
#         return data
    
#     def df_add(self,df):
#         if len(i:=self.df.index.intersection(df.index)):
#             self.lib.delete(self.symbol,chunk_range=i)
        
        
#         d=self.df.reindex(self.df.index.union(df.index))
#         d.loc[df.index]=df.loc[:]
#         self.lib.append(self.symbol,df)
#         self.df=d
        
#     async def binance_update(self):
#         async with self.lib.get_lock(self.symbol):
#             it=KLIterator2(self.symbol,self.lib.interval,self.df.index[-1])
#             async for df in it:
#                 self.df_add(df)
                # self.lib.append(self.symbol,df)
                # breakpoint()
                
                # self.df.update(df)
        

            

