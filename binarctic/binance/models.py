#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 22 20:17:28 2020

@author: ddd
"""

from typing import List,NamedTuple,TypeVar,Generic,NewType
from types import new_class
from operator import itemgetter
import functools as ft
import abc
from collections.abc import Mapping,Sequence
from datetime import timedelta,datetime
from pytz import timezone
from pprint import pprint,pformat
from inspect import ismethoddescriptor
from decimal import Decimal

# import typing




class ModelMeta(type):
    # def __new__(mtc, name, bases, ns):
    #     ns['__module__']=__name__
    #     return super().__new__(mtc, name, bases, ns)

        
    def __call__(cls,value):
        self=super().__call__(value)
        self.__value = value
        return self

class _ColMeta(ModelMeta,abc.ABCMeta):
    pass

class _Col(abc.ABC,metaclass=_ColMeta):
    __getitem__ = property(lambda self:self.__coll.__getitem__)
    __iter__ = lambda self:self.__coll.__iter__()
    __len__ = lambda self:self.__coll.__len__()
    
    __str__ = lambda self:self.__coll.__str__()
    __repr__ = lambda self:pformat(self.__coll)
    
    def __init__(self,coll):
        self.__coll=coll
    

class _Mapping(_Col,Mapping):

    def __init__(self,coll):
        super().__init__(dict(coll))
    

class _Sequence(_Col,Sequence):

    def __init__(self,coll):
        super().__init__(list(coll))

    

def Indexed(indexer):    
    def _decorator(cls):
        assert issubclass(cls,_Sequence)
        
        __init__=cls.__init__
        @ft.wraps(__init__)
        def _init(*args,**kwargs):
            __init__(*args,**kwargs)
            self , *args = args
            keys = map(indexer,self)
            self.__map=dict(zip(keys,self))
        cls.__init__ = _init
        cls.keys = property(lambda self : self.__map.keys)
        cls.get = property(lambda self : self.__map.get)
        
        def __getattr__(self,name):
            if name in self.__map:
                return self.__map[name]
            raise AttributeError(name)
        
        @ft.wraps(cls.__dir__)
        def __dir__(self):
            return __dir__.__wrapped__(self) + [k for k in self.__map]  
        
        cls.__getattr__ =  __getattr__
        cls.__dir__ = __dir__
        cls.__indexer__=staticmethod(indexer)
        return cls
    return _decorator

# def typed_list(fn=None,**kwargs):
#     if fn is None:
#         return lambda fn : typed_list(fn,**kwargs)
    
#     class TList(_Sequence):
#         __itemtype__=staticmethod(fn)
#         def __init__(self,value):
#             super().__init__((fn(i) for i in value))
#     # TList.__name__= kwargs.pop('name','TList')
#     TList.__module__ = fn.__module__
#     TList.__qualname__ = fn.__qualname__ + '.TList'
    
#     return TList

# class _Indexed_Sequence(_Sequence):
#     # __indexer__ = staticmethod(id)
    
#     def __init_subclass__(cls,**kwargs):
#         __indexer__ = kwargs.pop('indexer',None)
#         if callable(__indexer__):
#             # cls.__indexer__=staticmethod(__indexer__)
#             __init__=cls.__init__
#             def _init(self,value):
#                 __init__(self,value)
#                 keys = map(__indexer__,self)
#                 self.__map=dict(zip(keys,self))
#             cls.__init__ = _init
            
#         super().__init_subclass__(**kwargs)

   
#     # def __init__(self,coll):
#     #     super().__init__(coll)
#     #     keys = map(self.__indexer__,self)
#     #     self.__map=dict(zip(keys,self))

#     keys = property(lambda self : self.__map.keys)
#     get = property(lambda self : self.__map.get)
    
#     def __getattr__(self,name):
#         if name in self.__map:
#             return self.__map[name]
#         raise AttributeError(name)
        
#     def __dir__(self):
#         return super().__dir__() + [k for k in self.__map]
        
        
class _TypedDictMeta(_ColMeta):
    
    def __new__(mtc, name, bases, ns):
        anns = ns['__annotations__'] = ns.get('__annotations__', {})
        
        [ns.__setitem__(k,property(itemgetter(k))) for k in anns]
        
        for base in bases:
            anns.update(getattr(base,'__annotations__',{}))
        
        cls = super().__new__(mtc, name, bases, ns)
        return cls
    
        



    


    

class _TypedListMeta(_ColMeta):
    def __new__(mtc, name, bases, ns, itemtype=None,**kwargs):
        if itemtype:
            ns['__itemtype__'] = itemtype
        cls = super().__new__(mtc, name, bases, ns, **kwargs)
        return cls

    def __call__(cls,value,**kwargs):
        if not hasattr(cls,'__itemtype__'):
            name=kwargs.pop('name',value.__name__+'_List')
            return new_class(name,(cls,),{'itemtype':value,**kwargs})
                
        self=super().__call__(value,**kwargs)    
        return self
    

class TypedDict(_Mapping, metaclass=_TypedDictMeta):    
    def __init__(self,value):
        super().__init__(((k,t(value[k])) for  k,t in type(self).__annotations__.items()))
        
class TypedList(_Sequence, metaclass=_TypedListMeta):
    def __init__(self,value):
        super().__init__(map(type(self).__itemtype__, value))


        
# class TypedListIndexed(TypedList,_Indexed_Sequence):
#     pass       
    
    

    
        
def SimpleType(fn):
    class _Wrapper(metaclass=ModelMeta):
        def __new__(cls,value):
            return fn(value)
    return ft.wraps(fn,updated=())(_Wrapper)


@SimpleType
class datetime_ms(datetime):
    def __new__(cls,*args,**kwargs):
        return cls.fromtimestamp_ms(args[0]) if len(args)==1 else datetime.__new__(cls,*args,**kwargs)
    @classmethod
    def fromtimestamp_ms(cls, ms, tz=None):
        return cls.fromtimestamp(int(ms)/1000,tz)
    def timestamp_ms(self):
        return int(1000*self.timestamp())


@SimpleType    
class Bool(int):
    def __new__(cls,arg):
        return super().__new__(cls,bool(arg))
    def _m(sw):
        return lambda s,*args,**kwargs: sw.__get__(bool(s))(*args,**kwargs)
    for k,v in bool.__dict__.items():
        if ismethoddescriptor(v):
            locals()[k] = _m(v)
    del _m    

    
    
        
    
    
class RateLimit(TypedDict):
    rateLimitType : str 
    interval : str
    intervalNum : int
    limit: int
   
    def timedelta(self):
        return timedelta(**{self.interval.lower()+'s':self.intervalNum})
    def __repr__(self):
        return 'RateLimit {}: {} in {}'.format(self.rateLimitType,self.limit,self.timedelta())



class Symbol(TypedDict):
    symbol : str
    status : str
    baseAsset : str
    baseAssetPrecision : int
    quoteAsset : str
    quotePrecision : int
    quoteAssetPrecision : int
    baseCommissionPrecision : int
    quoteCommissionPrecision : int
    orderTypes : TypedList(str)
    icebergAllowed : bool
    ocoAllowed : Bool
    quoteOrderQtyMarketAllowed : bool
    isSpotTradingAllowed : bool
    isMarginTradingAllowed : bool
    filters : TypedList(dict)
    permissions : TypedList(str)
    
    def __repr__(self):
        return '%s <%s>' % (type(self).__name__, self.symbol)
    
@Indexed(itemgetter('symbol'))
class Symbols(TypedList, itemtype=Symbol):
    pass



class ExchangeInfo(TypedDict):
    timezone : timezone 
    serverTime : datetime_ms
    rateLimits : TypedList(RateLimit)
    exchangeFilters: TypedList(str)
    symbols : Symbols

    
class AggTrade(TypedDict):
    a : int
    p : Decimal
    q : Decimal
    f : int
    l : int
    T : datetime_ms
    m : Bool
    M : Bool
    
@Indexed(itemgetter('a'))
class AggTrades(TypedList, itemtype=AggTrade):
    pass


    
    
# if __name__=='__main__':  
#     from binarctic.binance.rest_api import Session
#     ei=Session().exchangeInfo()
#     e=ExchangeInfo(ei)
