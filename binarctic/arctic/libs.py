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
    def __init__(self,name,lib_type,on_create=None,**initkw):
        self.name=name
        self.lib_type=lib_type #if isinstance(lib_type,str) else lib_type.lib_type
        self.on_create = on_create if on_create else lambda x:None
        self.initkw=initkw
    
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
            lib = self._get(db)
            self.on_create(lib)
            return lib
    
    @classmethod
    def register_type(_cls,lib_type):
        def _wrapper(cls):
            try:
                arctic.register_library_type(lib_type,cls)
                # cls.lib_type=lib_type
                cls.factory=staticmethod(lambda name,**initkw : _cls(name, lib_type, **initkw))
                return cls
            except exceptions.ArcticException as e:
                raise e
                # print(e)
            
        
        return _wrapper
    

    
class LibSymbolMeta(type):
    @property
    class method(object):
        def __init__(self,cls):
            self.symbol_class=cls
        def __get__(self,ins,owner=None):
            if ins is None:
                return self.symbol_class
            return MethodType(self.symbol_class.create,ins)    
        
    @staticmethod
    def _create_mixin(lib_class):
        class _attr:
            def __init__(self,attr):
                self.attr=attr
                
            def __get__(self,ins,owner=None):
                if ins is None:
                    @ft.wraps(self.attr)
                    def _wrapper(ins,*args,**kwargs):
                        return self.__get__(ins)(*args,**kwargs)
                    return _wrapper
                fn=MethodType(self.attr,ins.lib)
                fn=MethodType(fn,ins.symbol)
                return fn
            

        
        def _upd(ns):
            for k in dir(lib_class):
                try:
                    a=getattr(lib_class,k)
                    a.__get__
                    s=signature(a)
                    param=list(s.parameters)
                    if param[1]=='symbol':
                    # if 'symbol' in s.parameters:
                        ns[k]=_attr(a)
                except (TypeError,ValueError,AttributeError,IndexError):
                    continue
        mixin=new_class(lib_class.__name__+".Symbol_Signature",(),{},_upd) 
        return mixin
    
    
    @property
    def registered_map(cls):
        try:
            return vars(cls)['_registered_map']
        except KeyError:
            cls._registered_map={}
            return cls._registered_map

    def create(cls,lib,symbol):
        return cls.registered_map.get(symbol,cls)(lib,symbol)
        
    def register_type(cls,symbol):
        def _dec(kls):
            if kls.__bases__!=(object,):
                raise TypeError('%s deberia ser (object,)'%kls.__bases__)
            if '__init__' in kls.__dict__:
                raise TypeError('__init__ no permitido')
                
            def __init__(self,lib,symbol=symbol):
                cls.__init__(self,lib,symbol)
                
            kls=type(cls)(kls.__name__,(kls,cls),{'__init__':__init__})
            
            kls.register_type = None
            kls._registered_map = None
            cls.registered_map[symbol]=kls
            # breakpoint()
            return kls
        return _dec
    
    

        
    
    

class LibSymbol(metaclass=LibSymbolMeta):
    
    def __init__(self,lib,symbol):
        self.lib=lib
        self.symbol=symbol

    def __repr__(self):
        return "%s <%s>" % (type(self).__qualname__,self.symbol)
    

 

    

    
    
class _LibBaseMeta(type):
    
    __Symbol__ = LibSymbol
        
    def __new__(mtc,name,bases,ns,**kwargs):
        if 'Symbol' in ns:
            raise AttributeError('Symbol not allowed')
            # ns['__Symbol__'] = ns.pop('Symbol')
        
        
        cls=super().__new__(mtc,name,bases,ns,**kwargs)
        
        Symbol = new_class(cls.__name__+".Symbol",
                          (cls.__Symbol__,LibSymbol._create_mixin(cls)),
                          {})
        
        cls.Symbol = Symbol
        
        return cls
    
    def register_symbol(cls,symbol):
        kls=cls.Symbol.register_type(symbol)
        return kls
    
class LibMeta(_LibBaseMeta):
    
    def __new__(mtc,name,bases,ns,**kwargs):
        cls=super().__new__(mtc,name,bases,ns,**kwargs)
        return cls 
    
    lib_class = property(lambda cls:cls)
        
    class metadata(Mapping):
        __slots__=['lib','_keys','_getitem','_setitem']
        def __init__(self,ins):
            self.lib = lib = ins.lib
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
         
        def __dir__(self):
            return super().__dir__() + list(self._keys())
        
        def __getattr__(self,name):
            try:
                return self[name]
            except KeyError:
                raise AttributeError(name)

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

class _LibBase(metaclass=_LibBaseMeta):
    
    metadata=property(LibMeta.metadata)
    custom_data=KeyProperty(metadata,CUSTOM_METADATA,dict(),allow_set=False,allow_delete=False)
    
    library = property(lambda self : self._arctic_lib.library)
    get_name = property(lambda self : self._arctic_lib.get_name)
    
    get_symbol = property(lambda self : MethodType(type(self).Symbol.create, self))


class Lib_Arctic(_LibBase,metaclass=LibMeta):
    
    lib = property(lambda self:self)
        
    @classmethod
    def initialize_library(cls,arctic_lib,custom_data=(),**kwargs):
        super().initialize_library(arctic_lib, **kwargs)
        arctic_lib.set_library_metadata(CUSTOM_METADATA,dict(custom_data))
        

     

    

    
        
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
    
    

MCHUNKSIZE = 'CHUNKSIZE'

@LibFactory.register_type("TickStoreV4")
class TickStore(Lib_Arctic,tickstore.tickstore.TickStore):
    
    @classmethod
    def initialize_library(cls,arctic_lib, chunk_size=100000,**kwargs):
        super().initialize_library(arctic_lib, **kwargs)
        arctic_lib.set_library_metadata(MCHUNKSIZE,chunk_size)
        
    def __init__(self,arctic_lib):
        super().__init__(arctic_lib,arctic_lib.get_library_metadata(MCHUNKSIZE))

        

@LibFactory.register_type("VersionStoreV4")
class VersionStore(Lib_Arctic,arctic.version_store.VersionStore):
    pass
@LibFactory.register_type("MetadataStoreV4")
class MetadataStore(Lib_Arctic,store.metadata_store.MetadataStore):
    pass
 
