#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  3 12:13:10 2020

@author: ddd
"""
from collections.abc import MutableMapping,MutableSequence

_empty=object()


class propagable(object):

    def __new__(cls,value):
        if isinstance(value,MutableMapping):
            return object.__new__(propagableMapping)
        elif isinstance(value,MutableSequence):
            return object.__new__(propagableSequence)
        else:
            return value    
    
    def __init__(self, value):    
        self.value=value
        
    def __iter__(self):
        return iter(self.value)
    
    def __len__(self):
        return len(self.value)
    
    def __getitem__(self,key):
        value=self.value[key]
        return PropagatedItem(self,key,value)
    
    def __setitem__(self,k,v):
        self.value[k]=v
        self._back_propagate()
    def __delitem__(self,k):
        del self.value[k]
        self._back_propagate()
    
    def __repr__(self):
        return "{} {}".format(type(self).__name__,self.value)
    
    def _back_propagate(self):
        pass
    
class propagableMapping(propagable,MutableMapping):
    def __dir__(self):
        return  list(self.keys()) + super().__dir__()
    
    def __getattr__(self,name):
        if name in self:
            return self.__getitem__(name)
        else:
            raise AttributeError(name)
            
class propagableSequence(propagable,MutableSequence):
    def insert(self,index,object):
        ret = self.value.insert(index,object)
        self._back_propagate()
        return ret

class PropagatedItem(propagable):
    def __new__(cls,parent,key,value):
        if isinstance(value,MutableMapping):
            return object.__new__(PropagatedMap)
        elif isinstance(value,MutableSequence):
            return object.__new__(PropagatedSequence)
        else:
            return value    
    
    def __init__(self, parent, key, value):
        super().__init__(value)
        self.parent=parent
        self.key=key
        
    def __repr__(self):
        return "{} key:{} value:{}".format(type(self).__name__,self.key,self.value)
    
    def _back_propagate(self):
        self.parent.__setitem__(self.key,self.value)
        
class PropagatedMap(PropagatedItem,propagableMapping):
    pass

class PropagatedSequence(PropagatedItem,propagableSequence):
    pass
