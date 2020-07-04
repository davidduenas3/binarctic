#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 21:38:11 2020

@author: ddd
"""

import functools as ft
from asyncio import sleep,Lock
from .libs import LibFactory,ChunkStore,KeyProperty
from .libwrapper import  wrapped_method,LibWrapper,ChunkStore_Wrapper
from ..binance.klines import KInterval,KLIterator2
from contextlib import asynccontextmanager
# from . import chunker

# def get_chunker(interval):
#     # from . import chunker
#     interval=KInterval(interval)
#     if interval.timedelta()<=interval.KLINE_INTERVAL_5MINUTE.timedelta():
#         return chunker.week_chunker
#     if interval.timedelta()<=interval.KLINE_INTERVAL_2HOUR.timedelta():
#         return chunker.month_chunker
#     return chunker.year_chunker

# class _Iterator(KLIterator2):
#     def __init__(self,lib,symbol):
#         self.lib=lib
#         super().__init__(symbol,lib.interval,lib.startTime(symbol))
        
    
MINTERVAL='INTERVAL'
        
@LibFactory.register_type('KLine_Lib')
class KlinesLib(ChunkStore):
    @classmethod
    def initialize_library(cls,arctic_lib,interval,chunker):
        ChunkStore.initialize_library(arctic_lib,chunker)
        arctic_lib.set_library_metadata(MINTERVAL,interval)
        
        
    @property
    def interval(self):
        interval=self._arctic_lib.get_library_metadata(MINTERVAL)
        return KInterval(interval)
    
    @property
    def locks(self):
        if not '_locks' in self.__dict__:
            self.__dict__['_locks']={}
        return self.__dict__['_locks']
    
    def get_lock(self,symbol):
        if not symbol in self.locks:
            self.locks[symbol]=Lock()
        return self.locks[symbol]
    

    
    def assert_symbols(lib,symbols):
        async def assert_sym(symbol):
            if lib._get_symbol_info(symbol) is None:
                it=KLIterator2(symbol,lib.interval,0,limit=100)
                df=await it.__anext__()
                lib.write(symbol,df)
        coros = map(assert_sym,symbols)
        return asyncio.gather(*coros)    

                    

        

    def startTime(self,symbol):
        # sym = self._get_symbol_info(symbol)
        if self._get_symbol_info(symbol):
            last=next(self.reverse_iterator(symbol))
            return last.index[-1]
        else:
            return 0
        
@LibFactory.register_type('KLine_Lib_Wrapper')
class KlinesLibWrapper(ChunkStore_Wrapper,lib_class=KlinesLib):
    pass
    # __lib_class__=KlinesLib
    
    
    
    
# print('type(KlinesLibWrapper): ',type(KlinesLibWrapper))
    
# KlinesLibWrapper=LibFactory.register_type('KLine_Lib_Wrapper')(LibWrapper.create_wrapper(KlinesLib))
    # @ft.partialmethod
    # class binance_iterator(KLIterator2):
    #     def __init__(self,lib,symbol):
    #         self.lib=lib
    #         super().__init__(symbol,lib.interval,lib.startTime(symbol))
    
    
    



        
        # iterator=