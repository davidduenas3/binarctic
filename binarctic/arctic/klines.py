#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 21:38:11 2020

@author: ddd
"""

import functools as ft
import pandas as pd
import threading
from asyncio import sleep,Lock,gather,create_task
from .libs import LibFactory,ChunkStore,KeyProperty,_LibBase
from .libwrapper import  wrapped_method,LibWrapper,ChunkStore_Wrapper,wrapped_attribute
# from ..binance.klines import KLIterator2,KInterval
from contextlib import asynccontextmanager,contextmanager
from collections import defaultdict
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

class WithBuffer(object):
    @property
    def _lock(self):
        if not '_lock' in self.__dict__:
            self.__dict__['_lock']=defaultdict(threading.Lock)
        return self.__dict__['_lock']
            
    locks = KeyProperty(_LibBase.custom_data,'LOCKS',list())
    
    @contextmanager
    def get_lock(self,symbol):
        if symbol in self.locks:
            raise ValueError("%s el locks!!")
        try:
            lock=self._lock[symbol]
            print(lock.locked())
            with lock:
                self.locks.append(symbol)
                yield lock
            # yield threading.Lock()
            
        finally:
            self.locks.remove(symbol)    
            
            
    
    
class LibBuffer(object):
    def __init__(self,lib,symbol):
        self.lib=lib
        self.symbol=symbol
        self.lock
        

    
MINTERVAL='INTERVAL'
        
@LibFactory.register_type('KLine_Lib')
class KlinesLib(ChunkStore,WithBuffer):
    @classmethod
    def initialize_library(cls,arctic_lib,interval,chunker):
        ChunkStore.initialize_library(arctic_lib,chunker)
        arctic_lib.set_library_metadata(MINTERVAL,interval)
        
        
    @property
    def interval(self):
        interval=self._arctic_lib.get_library_metadata(MINTERVAL)
        from ..binance.klines import KInterval
        return KInterval(interval)
    
    # locks = KeyProperty(ChunkStore.custom_data,'LOCKS',list())
    
    # @contextmanager
    # def get_lock(self,symbol):
    #     if symbol in self.locks:
    #         raise ValueError("%s el locks!!")
    #     try:
    #         self.locks.append(symbol)
    #         yield threading.Lock()
            
    #     finally:
    #         self.locks.remove(symbol)
    
    def get_appender(self,symbol):
        l=self.get_lock(symbol)
        class Appender:
            
            def append(s,df,flush=False):
                s.df=s.df.append(df)
                if flush:
                    s.flush()
                    
            def flush(s):
                if len(s.df):
                    print('flush')
                    self.append(symbol,s.df,upsert=True)
                    s.df=pd.DataFrame()
                    
            def __enter__(s):
                l.__enter__()
                s.df=pd.DataFrame()
                return s
            def __exit__(s,*args,**kwargs):
                s.flush()
                l.__exit__(*args,**kwargs)
                
        return Appender()
    
    def assert_symbol(self,symbol):
        if self._get_symbol_info(symbol) is None:
            it=KLIterator2(symbol,self.interval,0,limit=100)
            df=it.__next__()
            self.write(symbol,df)
             

    def binance_iterator(self,symbol,sess=None,**kwargs):
        if sess is None:
            from ..binance import Session
            sess=Session()
            
        if self._get_symbol_info(symbol):
            last=next(self.reverse_iterator(symbol))
            startTime = last.index[-1]
            flt=lambda df:df[df.index>startTime]
        else:
            startTime = 0
            flt=lambda df:df
        
        def _gen():
            with self.get_appender(symbol) as ap:
                it = sess.klines_iterator(symbol,self.interval,startTime,**kwargs)
                try:
                    ap.append(next(it).pipe(flt),True)
                    save = (yield ap)
                    while True:
                        ap.append(next(it),save)
                        save = (yield ap)
                    
                    
                except StopIteration as e:
                    print(e)

                    
        return _gen()           

        

    # def startTime(self,symbol):
    #     # sym = self._get_symbol_info(symbol)
    #     if self._get_symbol_info(symbol):
    #         last=next(self.reverse_iterator(symbol))
    #         return last.index[-1]
    #     else:
    #         return 0
        
    class __Symbol__(ChunkStore.__Symbol__):
        def __init__(self,lib,symbol):
            super().__init__(lib,symbol)
            self.assert_symbol()
        
    
    
@LibFactory.register_type('KLine_Lib_Wrapper')
class KlinesLibWrapper(ChunkStore_Wrapper,lib_class=KlinesLib):
    # assert_symbol = wrapped_method()
    # startTime = wrapped_method()
    interval = wrapped_attribute()
    _get_symbol_info = wrapped_method()
    read = wrapped_method()
    
    binance_iterator=wrapped_method()
    
    # class Symbol(ChunkStore_Wrapper.Symbol):
    #     def __init__(self,lib,symbol):
    #         super().__init__(lib,symbol)
    #         self.lib.assert_symbol_task(symbol)
            
        
    # __lib_class__=KlinesLib
    
    
    
    
# print('type(KlinesLibWrapper): ',type(KlinesLibWrapper))
    
# KlinesLibWrapper=LibFactory.register_type('KLine_Lib_Wrapper')(LibWrapper.create_wrapper(KlinesLib))
    # @ft.partialmethod
    # class binance_iterator(KLIterator2):
    #     def __init__(self,lib,symbol):
    #         self.lib=lib
    #         super().__init__(symbol,lib.interval,lib.startTime(symbol))
    
    
    



        
        # iterator=