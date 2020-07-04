#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 22:42:51 2020

@author: ddd
"""

import functools as ft
from inspect import (iscoroutine, ismethoddescriptor, isfunction, 
                     isasyncgen, isgenerator)

from types import MethodType, new_class

__all__= 'Pipable', 'apply_fn'

__empty=object()



def apply_fn(fn,
             res=__empty,
             *,
             relay_callable=True,
             ):
  
    def _wrapper(res):
        
        if ismethoddescriptor(res):
            return _methoddescriptor(res)
    
        if iscoroutine(res):
            return _coro(res)
        
        if isgenerator(res):
            return _gen(res)
        
        if isasyncgen(res):
            return _agen(res)
        
        if isfunction(res) or (relay_callable and callable(res)):
            return _func(res)
                
        return fn(res)    
            
    
    
    async def _coro(coro):
        return _wrapper(await coro)
        
    async def _agen(agen):     
        async for i in agen:            
            yield _wrapper(i)
    
    def _gen(gen):        
        for i in gen:            
            yield _wrapper(i)  
   
    def _func(func):
        def _call(*args,**kwargs):
            return _wrapper(func(*args,**kwargs))
        return ft.wraps(func)(_call)
    
    class _methoddescriptor:
        def __new__(cls,meth):
            self = object.__new__(type('_methoddescriptor',
                                       (_methoddescriptor,Pipable),
                                       {'__qualname__':type(meth).__qualname__}))
            return ft.wraps(meth)(self)

        def __get__(self,ins,owner=None):
            return self if ins is None else partial(self,ins)
        def __call__(self,ins,*args,**kwargs):
            ret=self.__wrapped__.__get__(ins)(*args,**kwargs)
            return _wrapper(ret)
                # __qualname__ = type(res).__qualname__
   
        
    _wrapper.__qualname__='apply_' + fn.__qualname__
    return _wrapper if res is __empty else _wrapper(res)


        
class Pipable(object):
    
    
    def __init_subclass__(cls):
        if hasattr(cls, "__get__") and not hasattr(cls, "__set__"):
            return 
        if hasattr(cls,'__call__'):
            return
        raise TypeError()
              
    def pipe(self,fn):
        return apply_fn(fn)(self)
    
    __or__ = pipe
    

    
def pipable(obj):
    if isinstance(obj, type):
        cls = new_class(obj.__name__, (obj, Pipable),{})
        cls.__module__ = __name__
        cls.__qualname__ = obj.__qualname__
        return cls
    else:
        raise TypeError()
        

partialmethod = pipable(ft.partialmethod)
partial = pipable(ft.partial)

# class partialmethod(ft.partialmethod,Pipable):
#     pass

    