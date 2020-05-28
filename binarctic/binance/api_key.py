#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 11 19:29:43 2020

@author: ddd
"""


import os
import functools as ft
import pandas as pd
import operator as op
import itertools as it
import pickle


from collections import namedtuple,abc

__all__=['api_config']

def assert_path(path):
    if not os.path.exists(path):
        with open(path,'wb') as f:
            keys={'Anon':('','')}
            default_key='Anon'
            pickle.dump((keys,default_key),f)    
    return path

PATH=os.path.join(os.path.expanduser('~'), '.binance_keys')
PATH=assert_path(PATH)
        
    
class api_config(abc.Mapping):

    Key=namedtuple('Key','api_key api_secret')
    
#    api_key=Key('','')
    
    def __init__(self,path=PATH):
        self._path=path
        self.load_keys(path)
#        Api_Config.api_key=self.default
       
    def __getitem__(self,key):
        if key=='default':
            key=self._default
        return self.Key(*self._keys[key])
    
#    get = property(lambda self:ft.partial(,default=self.default))
    
    def get(self,key,default='default'):
        return super().get(key,self[default])
#        try:
#            return self[key]
#        except KeyError:
#            return self.default
        
    def delete_key(self, alias):
        if alias in ['Anon',self._default,'default']:  
            raise ValueError(f'no se puede delete {alias}')
        del self._keys[alias]
        self.save_keys()
        
    def add_key(self,api_key,api_secret,alias=None,default=True,overwrite=False):
        alias = alias or api_key
        if alias in self and not overwrite:
            raise ValueError(f'ya existe {alias}')
        if alias in ['Anon',self._default, 'default']:  
            raise ValueError(f'no se puede overwrite {alias}')
        if default:
            self._default=alias
        self._keys[alias]= api_key,api_secret
        self.save_keys()
    def load_keys(self,path=None):
        path=path or self._path
        with open(path,'rb') as f:
            self._keys ,self._default = pickle.load(f)
        
    def save_keys(self,path=None):
        path=path or self._path
        with open(path,'wb') as f:
            keys=self._keys
            default_key=self._default
            pickle.dump((keys,default_key),f)
                    
    __getattr__=__getitem__
    
    def __iter__(self):
        return iter(self._keys)
    def __len__(self):
        return len(self._keys)
    
    def __dir__(self):
        return sorted(set(super().__dir__()) | set(self.keys()))
#    def __repr__(self):
#        return repr({**self})
    __call__=property(lambda self:self.__class__)
    
    def set_default(self,alias):
        if alias in self._keys:
            self._default=alias
            self.save_keys()

            
    default=property(lambda self:self['default'],
                     set_default)

api_config=api_config()
