#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 12:10:40 2020

@author: ddd
"""
import functools as ft
from types import new_class
from .libs import LibSymbol,ChunkStore,VersionStore,TickStore,LibFactory


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
            return getattr(owner.__wrapped__,self.name)
        return getattr(ins.__wrapped__,self.name)  
    

class wrapped_attribute(_wrapper):

    def __set__(self,ins,value):
        return setattr(ins.__wrapped__,self.name,value)
    
    def __delete__(self,ins):
        return delattr(ins.__wrapped__,self.name)


class LibWrapperSymbol(LibSymbol):
    def __init__(self,lib,symbol):
        super().__init__(lib.__wrapped__,symbol)
        
class LibWrapperMeta(type):
    __lib_class__=None
    
    @property
    def lib_class(cls):
        return cls.__lib_class__
    
    __wrapped__=lib_class
       
    __Symbol__=LibWrapperSymbol

    @classmethod
    def __prepare__(mtc, name, bases, lib_class=None, **kwargs):
        attrs=super().__prepare__(name, bases, **kwargs)
        
        if not any(filter(lambda b:isinstance(b,mtc),bases)):
            attrs['__wrapped__']=attrs['lib']=property(lambda self:self.__lib)
            attrs['__repr__'] = wrapped_method('__repr__')
            attrs['_arctic_lib']=wrapped_attribute('_arctic_lib')
            # attrs['metadata'] = wrapped_attribute('metadata')
            attrs['stats'] = wrapped_method('stats')
            
        if lib_class:
            attrs.update(__lib_class__=lib_class)
            # attrs['__lib_class__']=lib_class
            for lcls in (getattr(b,'lib_class') for b in bases if isinstance(b,mtc)):
                if lcls and not issubclass(lib_class,lcls):
                    raise TypeError("%s no es subclase de %s" % (lib_class,lcls))
            
        return attrs
            
    def __new__(mtc, name, bases, attrs, lib_class=None, **kwargs):

        # bases=*(b for b in bases if not b is LibWrapper),
        cls=super().__new__(mtc,name,bases,attrs,**kwargs)
        if cls.lib_class:  
            cls.Symbol = LibWrapperSymbol._extend(cls)._method()
        return cls
    
    # @property
    # def initialize_library(kls):
    #     if 'initialize_library' in kls.__dict__:
    #         return kls.__dict__['initialize_library'].__get__(None,kls)
    #     else:
    #         return kls.lib_class.initialize_library

    
    def __call__(cls,arctic_lib, **kwargs):
        if cls.lib_class:
            self = super().__call__()
            self.__lib=cls.lib_class(arctic_lib, **kwargs)
            return self
        raise TypeError('lib_class??')
    



class LibWrapper(metaclass=LibWrapperMeta):
    
    list_symbols = wrapped_method()
    initialize_library = wrapped_method()
    get_name = wrapped_method()
     
    metadata = wrapped_attribute()
    custom_data = wrapped_attribute()
   

@LibFactory.register_type('ChunkStore_Wrapper')
class ChunkStore_Wrapper(LibWrapper,lib_class=ChunkStore):
    read = wrapped_method('read')
    pass
    
@LibFactory.register_type('TickStore_Wrapper')
class TickStore_Wrapper(LibWrapper,lib_class=TickStore):
    pass

@LibFactory.register_type('VersionStore_Wrapper')
class VersionStore_Wrapper(LibWrapper,lib_class=VersionStore):
    pass
    # __repr__ = wrapped_libmethod('__repr__')
    # _arctic_lib = wrapped_attribute('_arctic_lib')
    # stats = wrapped_libmethod('stats')