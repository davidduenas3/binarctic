#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 22 20:17:28 2020

@author: ddd
"""

from typing import List,NamedTuple,TypeVar,Generic,NewType
from types import new_class, MappingProxyType
from operator import itemgetter
import functools as ft
import itertools as itt
import abc
import pandas as pd
from collections.abc import Mapping,Sequence,AsyncIterable,Iterable
from datetime import timedelta,datetime
from pytz import timezone
from pprint import pprint,pformat
from inspect import ismethoddescriptor
from decimal import Decimal
from binarctic.utils import *
from dateutil.parser import parse
# class datetime_ms(datetime):
#     def __new__(cls,*args,**kwargs):
#         return cls.fromtimestamp_ms(args[0]) if len(args)==1 else datetime.__new__(cls,*args,**kwargs)
#     @classmethod
#     def fromtimestamp_ms(cls, ms):
#         return cls.utcfromtimestamp(int(ms)/1000)
#     def timestamp_ms(self):
#         return int(1000*self.timestamp())
    
#     [
#                     1499040000000,      # Open time
#                     "0.01634790",       # Open
#                     "0.80000000",       # High
#                     "0.01575800",       # Low
#                     "0.01577100",       # Close
#                     "148976.11427815",  # Volume
#                     1499644799999,      # Close time
#                     "2434.19055334",    # Quote asset volume
#                     308,                # Number of trades
#                     "1756.87402397",    # Taker buy base asset volume
#                     "28.46694368",      # Taker buy quote asset volume
#                     "17928899.62484339" # Can be ignored
#                 ]
    


# class Tstamp(pd.Timestamp):
#     def __new__(cls,*args,**kwargs):
#         self=super().__new__(cls,*args,**kwargs)
#         self.__class__=cls
#         return self

class tsms(int):
    __slots__=()
    def __new__(cls,arg):
        if isinstance(arg,datetime):
            arg=int(1000*arg.timestamp())
            
        self = super().__new__(cls,arg)
        # self._as_datetime=pd.Timestamp.utcfromtimestamp(self/1000)
        return self
    
    def to_datetime(self):
        return pd.Timestamp.utcfromtimestamp(self/1000)
    
    @classmethod
    def from_datetime(cls,dt):
        if isinstance(dt,str):
            dt=pd.Timestamp(parse(dt))
        return cls(int(1000*dt.timestamp()))
    
    def __str__(self):
        return str(int(self))
    
    def __repr__(self):
        return '{} <-> {}'.format(
            int(self),self.to_datetime()._short_repr)


    
    

    



# a['dsf']
# def KLine_DF(iterable):
    
#     def to_dict(i):
#         return dict(
#             t=datetime_ms(i[0]),
#             o=float(i[1]),
#             h=float(i[2]),
#             l=float(i[3]),
#             c=float(i[4]),
#             v=float(i[5]),
#             tclose=datetime_ms(i[6]),
#             ntrades=int(i[8])
#             )
    
#     df=pd.DataFrame(map(to_dict,iterable)).set_index('t')
#     return df
    
# df=KLine_DF(kk)
# class ModelMeta(type):
  
#     def __call__(cls,value):
#         self=super().__call__(value)
#         self.__value = value
#         return self

# class _ColMeta(ModelMeta,abc.ABCMeta):
#     pass

# class _Col(abc.ABC,metaclass=_ColMeta):
#     __getitem__ = property(lambda self:self.__coll.__getitem__)
#     __iter__ = lambda self:self.__coll.__iter__()
#     __len__ = lambda self:self.__coll.__len__()
    
#     __str__ = lambda self:self.__coll.__str__()
#     __repr__ = lambda self:pformat(self.__coll)
    
#     def __init__(self,coll):
#         self.__coll=coll
    

# class _Mapping(_Col,Mapping):

#     def __init__(self,coll):
#         super().__init__(dict(coll))
    

# class _Sequence(_Col,Sequence):

#     def __init__(self,coll):
#         super().__init__(list(coll))

    

# def Indexed(indexer):    
#     def _decorator(cls):
#         assert issubclass(cls,_Sequence)
        
#         __init__=cls.__init__
#         @ft.wraps(__init__)
#         def _init(*args,**kwargs):
#             __init__(*args,**kwargs)
#             self , *args = args
#             keys = map(indexer,self)
#             self.__map=dict(zip(keys,self))
#         cls.__init__ = _init
#         cls.keys = property(lambda self : self.__map.keys)
#         cls.get = property(lambda self : self.__map.get)
        
#         def __getattr__(self,name):
#             if name in self.__map:
#                 return self.__map[name]
#             raise AttributeError(name)
        
#         @ft.wraps(cls.__dir__)
#         def __dir__(self):
#             return __dir__.__wrapped__(self) + [k for k in self.__map]  
        
#         cls.__getattr__ =  __getattr__
#         cls.__dir__ = __dir__
#         cls.__indexer__=staticmethod(indexer)
#         return cls
#     return _decorator


        
# class _TypedDictMeta(_ColMeta):
#     # @classmethod
#     # def __prepare__(mtc,name,bases):
#     #     class _TDICT(dict):
#     #         def __setitem__(s,k,v):
#     #             if k=='__annotations__':
#     #                 s['_names_map_']={n:n for n in v}
#     #                 # pass
#     #                 # print(v)
#     #             super().__setitem__(k,v)
                    
                        
#     #         def __getitem__(s,k):
#     #             if k=='__annotations__':
                    
#     #                 # pass
#     #                 # breakpoint()
#     #                 print(k,list(super().__getitem__(k)))
#     #             return super().__getitem__(k)
#     #     return _TDICT()
        
#     def __new__(mtc, name, bases, ns):
#         anns = ns.get('__annotations__', {})
        
#         [ns.__setitem__(k,property(itemgetter(k))) for k in anns]
        
#         # for base in bases:
#         #     anns.update(getattr(base,'__annotations__',{}))
        
#         cls = super().__new__(mtc, name, bases, ns)
#         return cls
    
        

# class _TypedListMeta(_ColMeta):
#     def __new__(mtc, name, bases, ns, itemtype=None,**kwargs):
#         if itemtype:
#             ns['__itemtype__'] = itemtype
#         cls = super().__new__(mtc, name, bases, ns, **kwargs)
#         return cls

#     def __call__(cls,value,**kwargs):
#         if not hasattr(cls,'__itemtype__'):
#             name=kwargs.pop('name',value.__name__+'_List')
#             return new_class(name,(cls,),{'itemtype':value,**kwargs})
                
#         self=super().__call__(value,**kwargs)    
#         return self
    

    


# class TypedDict(_Mapping, metaclass=_TypedDictMeta):
#     def __init__(self,value):
#         super().__init__(((k,t(value[k])) for  k,t in type(self).__annotations__.items()))
        
# class TypedList(_Sequence, metaclass=_TypedListMeta):
#     def __init__(self,value):
#         super().__init__(map(type(self).__itemtype__, value))


# class TypedTuple(TypedDict):
#     def __init__(self,value):
#         d={k:value[i] for i,k in enumerate(type(self).__annotations__)}
#         super().__init__(d)
    
     
# # class TypedListIndexed(TypedList,_Indexed_Sequence):
# #     pass       
    
    

    
        
# def SimpleType(fn):
#     class _Wrapper(metaclass=ModelMeta):
#         def __new__(cls,value):
#             return fn(value)
#     return ft.wraps(fn,updated=())(_Wrapper)


# @SimpleType
# class datetime_ms(datetime):
#     def __new__(cls,*args,**kwargs):
#         return cls.fromtimestamp_ms(args[0]) if len(args)==1 else datetime.__new__(cls,*args,**kwargs)
#     @classmethod
#     def fromtimestamp_ms(cls, ms, tz=None):
#         return cls.fromtimestamp(int(ms)/1000,tz)
#     def timestamp_ms(self):
#         return int(1000*self.timestamp())


# @SimpleType    
# class Bool(int):
#     def __new__(cls,arg):
#         return super().__new__(cls,bool(arg))
#     def _m(sw):
#         return lambda s,*args,**kwargs: sw.__get__(bool(s))(*args,**kwargs)
#     for k,v in bool.__dict__.items():
#         if ismethoddescriptor(v):
#             locals()[k] = _m(v)
#     del _m    

    
    
        
    
    
# class RateLimit(TypedDict):
#     rateLimitType : str 
#     interval : str
#     intervalNum : int
#     limit: int
   
#     def timedelta(self):
#         return timedelta(**{self.interval.lower()+'s':self.intervalNum})
#     def __repr__(self):
#         return 'RateLimit {}: {} in {}'.format(self.rateLimitType,self.limit,self.timedelta())



# class Symbol(TypedDict):
#     symbol : str
#     status : str
#     baseAsset : str
#     baseAssetPrecision : int
#     quoteAsset : str
#     quotePrecision : int
#     quoteAssetPrecision : int
#     baseCommissionPrecision : int
#     quoteCommissionPrecision : int
#     orderTypes : TypedList(str)
#     icebergAllowed : bool
#     ocoAllowed : Bool
#     quoteOrderQtyMarketAllowed : bool
#     isSpotTradingAllowed : bool
#     isMarginTradingAllowed : bool
#     filters : TypedList(dict)
#     permissions : TypedList(str)
    
#     def __repr__(self):
#         return '%s <%s>' % (type(self).__name__, self.symbol)
    
# @Indexed(itemgetter('symbol'))
# class Symbols(TypedList, itemtype=Symbol):
#     pass



# class ExchangeInfo(TypedDict):
#     timezone : timezone 
#     serverTime : datetime_ms
#     rateLimits : TypedList(RateLimit)
#     exchangeFilters: TypedList(str)
#     symbols : Symbols

    


# class KLine(TypedTuple):
#     opentime : datetime_ms 
#     o : Decimal
#     h : Decimal
#     l : Decimal
#     c : Decimal
#     v : Decimal
#     closetime : datetime_ms
#     quote_volume : Decimal
#     ntrades : int
#     tbuybasevolume : Decimal
#     takerbuyassetvolume : Decimal
#     _ignore : str
    
#     def __repr__(self):
#         return '%s <%s>' % (type(self).__name__, self.opentime)
    

# class KLines(TypedList, itemtype=KLine):
#     pass



# from attr import  attrs,attrib,dataclass
# import attr


# # def typed_dict(cls,)

# # def typed_attrib(type):
# #     fn = ft.partial(attr.ib,type=type,converter=type)
# #     def _wrapper(*args,**kwargs):
# #         ret = fn(*args,**kwargs)
# #         # breakpoint()
# #         return ret
        
# #     return ft.wraps(fn)(_wrapper)
# #     # def _wrapper(*args,**kwargs):
# #     #     return attr.ib(*args,type=type,converter=type,**kwargs)
# #     # return _wrapper
# # int_attrib=typed_attrib(int)
# # Decimal_attrib=typed_attrib(Decimal)
# # bool_attrib = typed_attrib(bool)


    
# def typed_sequence(cls=None,itemtype=None):
#     if cls is None:
#         return lambda cls:typed_sequence(cls,itemtype)
#     cls._itemtype = itemtype or cls._itemtype
    
#     _init=cls.__init__
#     @ft.wraps(_init)
#     def _init_(self,*args,**kwargs):
#         if args:
#             iterable,*args = args
#         else:
#             iterable =()
            
#         self.__list=list(iterable)
#         _init(self,*args,**kwargs)
    
#     cls.__init__=_init_  

#     def __len__(self):
#         return len(self.__list)
#     cls.__len__=__len__
    
#     def __getitem__(self,ix):
#         if isinstance(ix,int):
#             return cls._itemtype(self.__list[ix])
#         else:
#             return [self[i] for i in range(*ix.indices(len(self)))]
#     cls.__getitem__= __getitem__
    
#     cls.__repr__=lambda self:pformat(self[:])
    
#     if not issubclass(cls,Sequence):
#         cls.__bases__=(Sequence,*cls.__bases__)
#     return cls



# @attrs(frozen=True,slots=True)
# class AggTrade(object):
#     a = attrib()
#     p = attrib(converter=float)
#     q = attrib(converter=float)
#     f = attrib(repr=False)
#     l = attrib(repr=False)
#     T = attrib(converter=datetime_ms)
#     m = attrib(repr=False)
#     M = attrib(repr=False)

#     @classmethod
#     def list_of(cls,iterable):
#         return list(attr.asdict(cls(**i))for i in iterable)
    
# # class AggTrade(AggTrade):
# #     __slots__=()
# #     def __init__(self,mapping):
# #         super().__init__(**mapping)
    

# class AggTrades(Sequence):
#     __itemtype__ = __itembuild__ = AggTrade
#     __getitem__ = property(lambda self:self._data.__getitem__)
#     __len__ = lambda self:len(self._data)
#     __iter__ = lambda self:iter(self._data)
    
#     def __repr__(self):
#         return pformat(self._data,compact=True)
    
#     def __init__(self,iterable):
#         self._data=list(attr.asdict(AggTrade(**i))for i in iterable)
#         # if isinstance(iterable,type(self)):
#         #     self._data[:] = iterable._data
#         # else:
#         #     self._extend(iterable)
            
#     # def _extend(self,iterable):
#     #     if isinstance(iterable,type(self)):
#     #         self._data.extend(iterable._data)
#     #     else:
#     #         for i in map(self.__itemtype__,iterable):
#     #             self._data.append(i)
            
#     # def _append(self,e):
#     #     if isinstance(iterable,self.__itemtype__):
#     #         self._data.append(e)
#     #     else:
#     #         raise ValueError('%s not is instance of %s' % (e,self.__itemtype__))
            
    
#     # @classmethod
#     # def from_dict(cls,*args,**kwargs):
#     #     return cls(**dict(*args,**kwargs))


#     # def __repr__(self):
#     #     def _repr(att):
#     #         return f'{att.name}={str(getattr(self,att.name))}'
#     #     return type(self).__name__ + '(' + ', '.join(
#     #         (_repr(a) for a in attr.fields(type(self)) if a.repr)
#     #         ) + ')'   

# # @typed_sequence(itemtype=AggTrade.from_dict)
# # class AggTrades:
# #     # def __init__(self):
# #     #     print(self[:])
# #     #     # pass
# #     pass
    
 
    

    
# agg={
#         "a": 26129,         # Aggregate tradeId
#         "p": "0.01633102",  # Price
#         "q": "4.70443515",  # Quantity
#         "f": 27781,         # First tradeId
#         "l": 27781,         # Last tradeId
#         "T": 1498793709153, # Timestamp
#         "m": True,          # Was the buyer the maker?
#         "M": True           # Was the trade the best price match?
#     }
# attr.fields(AggTrade)
# @Indexed(itemgetter('a'))
# class AggTrades(TypedList, itemtype=AggTrade):
#     pass
# if __name__=='__main__':  
#     from binarctic.binance.rest_api import Session
#     ei=Session().exchangeInfo()
#     e=ExchangeInfo(ei)
