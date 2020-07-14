#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 12:10:40 2020

@author: ddd
"""
import functools as ft
from types import new_class,MethodType,FunctionType
from inspect import isfunction,ismethod,signature
from .libs import LibSymbol,ChunkStore,VersionStore,TickStore,LibFactory,_LibBaseMeta,_LibBase,MetadataStore


class _wrapper(object):
    
    def __init__(self,name=None):
        self.name=name
        
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        return getattr(ins.__wrapped__,self.name)
    
    def __set_name__(self,owner,name):
        self.name=self.name or name

class wrapped_method(_wrapper):
    def __get__(self,ins,owner=None):
        if ins is None:
            fn=getattr(owner.__wrapped__,self.name)
            @ft.wraps(fn)
            def _wrapper(*args,**kwargs):
                if ismethod(fn):
                    return fn(*args, **kwargs)
                elif isfunction(fn):
                    assert 'self' in signature(fn).parameters
                    ins,*args=args
                    return fn(ins.__wrapped__,*args,**kwargs)
            return _wrapper

        fn=getattr(ins.__wrapped__,self.name)
        @ft.wraps(fn)
        def _wrapper(*args,**kwargs):
            return fn(*args,**kwargs)
        return _wrapper
     

class wrapped_attribute(_wrapper):

    def __set__(self,ins,value):
        return setattr(ins.__wrapped__,self.name,value)
    
    def __delete__(self,ins):
        return delattr(ins.__wrapped__,self.name)


class LibWrapperSymbol(LibSymbol):
   
    def __init__(self, lib, symbol):
        super().__init__(lib, symbol)
        self.__wrapped__=self.lib.__wrapped__.get_symbol(symbol)
        
    
    @property
    def libSymbol(self):
        return self.__wrapped__
        
class LibWrapperMeta(_LibBaseMeta):
    
    __lib_class__=None
    
    __wrapped__ = lib_class = property(lambda cls:cls.__lib_class__)
       
    __Symbol__ = LibWrapperSymbol
    

        
    # @classmethod
    # def __prepare__(mtc, name, bases, lib_class=None, **kwargs):
    #     attrs=super().__prepare__(name, bases, **kwargs)
        
    #     if not any(filter(lambda b:isinstance(b,mtc),bases)):
    #         attrs['__wrapped__']=attrs['lib']=property(lambda self:self.__lib)
            
    #     if lib_class:
    #         attrs.update(__lib_class__=lib_class)
    #         # attrs['__lib_class__']=lib_class
    #         for lcls in (getattr(b,'lib_class') for b in bases if isinstance(b,mtc)):
    #             if lcls and not issubclass(lib_class,lcls):
    #                 raise TypeError("%s no es subclase de %s" % (lib_class,lcls))
            
    #     return attrs
            
    def __new__(mtc, name, bases, attrs, lib_class=None, **kwargs):
        attrs['__wrapped__'] = attrs['lib'] = property(lambda self:self.__lib)
        
        if lib_class:
            attrs['__lib_class__']=lib_class
            for lcls in (getattr(b,'lib_class') for b in bases if isinstance(b,mtc)):
                if lcls and not issubclass(lib_class,lcls):
                    raise TypeError("%s no es subclase de %s" % (lib_class,lcls))   
                    
        cls=super().__new__(mtc,name,bases,attrs,**kwargs)
        return cls
    
    
    
    def __call__(cls,arctic_lib, **kwargs):
        if cls.lib_class:
            self = cls.__new__(cls, **kwargs)
            self.__lib=cls.lib_class(arctic_lib)
            self.__init__(**kwargs)
            return self
        raise TypeError('lib_class??')
    



class LibWrapper(_LibBase,metaclass=LibWrapperMeta):
    
    has_symbol = wrapped_method()       
    list_symbols = wrapped_method()
    

    stats = wrapped_method()
    
    initialize_library = wrapped_method()
    _arctic_lib=wrapped_attribute()
    
    def __repr__(self):
        return "<%s at %s>\n%s" %(type(self).__name__,hex(id(self)),repr(self.__wrapped__))
    

    
    

@LibFactory.register_type('ChunkStore_Wrapper')
class ChunkStore_Wrapper(LibWrapper,lib_class=ChunkStore):
    chunker = wrapped_attribute()
    
    
@LibFactory.register_type('TickStore_Wrapper')
class TickStore_Wrapper(LibWrapper,lib_class=TickStore):
    _chunk_size = wrapped_attribute()
    

@LibFactory.register_type('VersionStore_Wrapper')
class VersionStore_Wrapper(LibWrapper,lib_class=VersionStore):
    pass

@LibFactory.register_type("MetadataStore_Wrapper")
class MetadataStore_Wrapper(LibWrapper,lib_class=MetadataStore):
    read = wrapped_method()
    read_history = wrapped_method()