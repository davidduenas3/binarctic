#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 12:09:01 2020

@author: ddd
"""


# from binarctic.binance import Session,ASession
import pandas as pd
from binarctic.binance.klines import KLIterator2

sym='BTCUSDT'
symbols=[sym,'BNBBTC']
# i = KLIterator2(sym,'1m')


from binarctic.arctic import Arctic,DateRange
# from arctic.date import Daterange

a=Arctic()
# del a.klines_1h
import asyncio

async def update(symbol):
    
    # del a.klines_1m
    lib=a.klines_1m

    async def binance_update():
        async with lib.get_lock(symbol):
            it=KLIterator2(symbol,lib.interval,lib.startTime(symbol))
            async for df in it:
                lib.update(symbol,df,chunk_range=df.index,upsert=True)
                print(it.startTime)
                
    while True:
        await binance_update()
        await asyncio.sleep(1)

    
    
# task=asyncio.create_task(update(sym))

class KLineUpdater(object):
    @classmethod
    def assert_symbols(cls,lib,symbols):
        async def assert_sym(symbol):
            if lib._get_symbol_info(symbol) is None:
                it=KLIterator2(symbol,lib.interval,0,limit=100)
                df=await it.__anext__()
                lib.write(symbol,df)
        coros = map(assert_sym,symbols)
        return asyncio.gather(*coros)
    
    def __init__(self,lib,symbol):
        self.lib=lib
        self.symbol=symbol
        if self.lib._get_symbol_info(symbol) is None:
            raise KeyError("%s not found" % symbol)
        self.df=self.read()
        self.last=self.df.index[-1]
        
    def read(self,limit=100000):
        data=pd.DataFrame()
        for df in self.lib.reverse_iterator(self.symbol):
            data=pd.concat([df,data])
            if len(data)>limit:
                data=data[-limit:]
                break
        return data
    
    def df_add(self,df):
        if len(i:=self.df.index.intersection(df.index)):
            self.lib.delete(self.symbol,chunk_range=i)
        
        
        d=self.df.reindex(self.df.index.union(df.index))
        d.loc[df.index]=df.loc[:]
        self.lib.append(self.symbol,df)
        self.df=d
        
    async def binance_update(self):
        async with self.lib.get_lock(self.symbol):
            it=KLIterator2(self.symbol,self.lib.interval,self.df.index[-1])
            async for df in it:
                self.df_add(df)
                # self.lib.append(self.symbol,df)
                # breakpoint()
                
                # self.df.update(df)
        

            
# updater=KLineUpdater(a.klines_1m,sym)
# task=asyncio.create_task(updater.binance_update())
# t=asyncio.create_task(updater.binance_update())
# class KLineDF:
#     lib=a.klines_1m
    

        
#     def __init__(self,symbol):
#         if self.lib._get_symbol_info(symbol) is None:
#             raise KeyError("%s not found" % symbol)
#         self.symbol=symbol
#         self._df = self.lib.read(symbol)
        
#     @property
#     def df(self):
#         self._update()
#         return self._df
    
#     def _update(self):
        
#         df=self._df
#         u=self.lib.read(symbol,chunk_range=DateRange(df.index[-1]))
#         if not (i:=df.index.intersection(u.index)).empty:
#             df[i]=u[i]
        
#         for dt in df.index.intersection(u.index):
#             df[dt]=u[dt]
        
        
        
    
    
    
    
    
   
# lib=a.klines_1h

# lib.write(sym,df)

# lib.lib.read(sym)
# lib=a.klines_1m

