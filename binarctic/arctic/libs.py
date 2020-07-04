#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 22:48:13 2020

@author: ddd
"""

import functools as ft
from types import new_class,MethodType
from arctic import arctic,exceptions,chunkstore,tickstore,store,register_library_type
from collections.abc import MutableMapping,Mapping,MutableSequence
from inspect import signature,isfunction

from ..propagable import PropagatedItem

class LibFactory(object):
    def __init__(self,name,lib_type,**kwargs):
        self.name=name
        self.lib_type=lib_type #if isinstance(lib_type,str) else lib_type.lib_type
        self.initkw=kwargs
    
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        return self._get(ins)
    
    def __set__(self,ins,value):
        raise AttributeError("Can't set attribute")
        
    def __delete__(self,db):
        db.delete_library(self.name)
        
    def _get(self,db):
        try:
            return db.get_library(self.name)
        except arctic.LibraryNotFoundException:
            db.initialize_library(self.name,self.lib_type,**self.initkw)
            return self._get(db)
    
    @classmethod
    def register_type(_cls,lib_type):
        def _wrapper(cls):
            try:
                arctic.register_library_type(lib_type,cls)
                cls.lib_type=lib_type
                cls.factory=staticmethod(lambda name,**initkw:_cls(name,lib_type,**initkw))
                return cls
            except exceptions.ArcticException as e:
                raise e
                # print(e)
            
        
        return _wrapper
    

    


class LibSymbol(object):
    def __init__(self,lib,symbol):
        self.lib=lib
        self.symbol=symbol
        
    @classmethod
    class _method(object):
        def __init__(self,cls):
            self.symbol_class=cls
        def __get__(self,ins,owner=None):
            if ins is None:
                return self.symbol_class
            return MethodType(self.symbol_class,ins)
 
    @staticmethod
    def _extend(lib_class):
        class _attr:
            def __init__(self,attr):
                self.attr=attr
            def __get__(self,ins,owner=None):
                if ins is None:
                    return self
                fn=self.attr.__get__(ins.lib)
                # fn=getattr(lib_class,self.name).__get__(ins.lib)
                return ft.partial(fn,symbol=ins.symbol)
                
        def _upd(ns):
            for k in dir(lib_class):
                try:
                    a=getattr(lib_class,k)
                    a.__get__
                    s=signature(a)
                    # p=s.parameters
                    if 'symbol' in s.parameters:
                        ns[k]=_attr(a)
                except (TypeError,ValueError,AttributeError):
                    continue
        mixin=new_class(lib_class.__name__+".Symbol_Signature",(),{},_upd)        
        # name = lib_class.__name__+".Symbol"
        cls = new_class(lib_class.__name__+".Symbol",(lib_class.__Symbol__,mixin),{})
        return cls


class LibMeta(type):

    def __new__(mtc,name,bases,ns,**kwargs):
        cls=super().__new__(mtc,name,bases,ns,**kwargs)
        cls.Symbol = LibSymbol._extend(cls)._method()
        # cls._createSymbol()
        return cls
        

    __Symbol__ = LibSymbol
    
    
        
    class metadata(Mapping):
        def __init__(self,lib):
            self.lib=lib
            self._keys = lib._arctic_lib.library_metadata_keys
            self._getitem = lib._arctic_lib.get_library_metadata
            self._setitem = lib._arctic_lib.set_library_metadata
        
        def __len__(self):
            return len(self._keys())
        
        def __iter__(self):
            return iter(self._keys())
        def __repr__(self):
            return "{} <{}>".format(type(self).__name__,self.lib._arctic_lib.get_name())
        
        def __getitem__(self,key):
            if not key in self._keys():
                raise KeyError(key)
            value = self._getitem(key)
            return PropagatedItem(self,key,value)

        
        def __delitem__(self,key):
            raise AttributeError('__delitem__')
            
        def __setitem__(self,key,value):
            self._setitem(key,value)
            
        
        # @classmethod
        # class descriptor(object):
        #     def __init__(self,mdataclass):
        #         ft.wraps(mdataclass)(self)
                
        #     def __get__(self,instance,owner=None):
        #         if instance is None:
        #             return self
        #         return self.__wrapped__(instance)
            


    # def Wrapper(cls):
    #     from .libwrapper import LibWrapperMeta
        # return LibWrapperMeta.create_wrapper(cls)
    
    # def extend_wrapper(cls,base):
    #     from .libwrapper import LibWrapperMeta
    #     return LibWrapperMeta.extend_wrapper(cls)(base)
        

CUSTOM_METADATA='CUSTOM_METADATA'

class KeyProperty(object):
    def __init__(self,desc, key,
                 default=None, 
                 allow_set=True,
                 allow_delete=True):
        self.desc=desc
        self.key=key
        self.default = default if callable(default) or default is None else lambda s:default
        self.allow_set=allow_set
        self.allow_delete=allow_delete
        # self.getter=getter
        
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        map=self.desc.__get__(ins,owner)
        if not self.key in map and not self.default is None:
            map[self.key]=self.default(ins)
        value = map[self.key]
        return value 
    
    def __set__(self,ins,value):
        if not self.allow_set:
            raise AttributeError(self.key)
        map=self.desc.__get__(ins)
        map[self.key] = value 
        
    def __delete__(self,ins):
        if not self.allow_delete:
            raise AttributeError(self.key)
        map=self.desc.__get__(ins)
        del map[self.key]
    
    def _defaultsetter(self,fn):
        kw={**vars(self),'default':fn}
        return type(self)(**kw)
    
    @classmethod
    def Default(cls,desc,key,**kwargs):
        return cls(desc,key,**kwargs)._defaultsetter


class Lib_Arctic(metaclass=LibMeta):
    
    metadata=property(LibMeta.metadata)
    
    custom_data=KeyProperty(metadata,CUSTOM_METADATA,dict(),allow_set=False,allow_delete=False)
    

    @classmethod
    def initialize_library(cls,arctic_lib,custom_data=(),**kwargs):
        super().initialize_library(arctic_lib, **kwargs)
        arctic_lib.set_library_metadata(CUSTOM_METADATA,dict(custom_data))
        
        
    def get_name(self):
        return self._arctic_lib.get_name()
    

        
        
MCHUNKER = 'CHUNKER'

@LibFactory.register_type("ChunkStoreV4")
class ChunkStore(Lib_Arctic,chunkstore.ChunkStore):
    
    @classmethod
    def initialize_library(cls,arctic_lib, chunker=chunkstore.DateChunker.TYPE,**kwargs):
        super().initialize_library(arctic_lib, **kwargs)
        arctic_lib.set_library_metadata(MCHUNKER,chunker)
        
        # return cls.lib_class.initialize_library(arctic_lib,**kwargs)
    
    @property
    def chunker(self):
        name = self._arctic_lib.get_library_metadata(MCHUNKER)
        return chunkstore.get_chunker(name)
    
    @property
    def write(self):
        return ft.partial(super().write,chunker=self.chunker)

@LibFactory.register_type("TickStoreV4")
class TickStore(Lib_Arctic,tickstore.tickstore.TickStore):
    pass

@LibFactory.register_type("VersionStoreV4")
class VersionStore(Lib_Arctic,arctic.version_store.VersionStore):
    pass

 
