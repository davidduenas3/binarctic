#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 10:11:03 2020

@author: ddd
"""


sym='BTCUSDT'
symbols=[sym,'BNBBTC']
# i = KLIterator2(sym,'1m')


from binarctic.arctic import Arctic,DateRange
# from arctic.date import Daterange

a=Arctic()

names=[k for k in Arctic.__dict__ if k.endswith('store')]
names_w=[k for k in Arctic.__dict__ if k.endswith('store_w')]

tick = *map(lambda n : n[0],(names,names_w)),
chunk = *map(lambda n : n[1],(names,names_w)), #+ names_w
vers = *map(lambda n : n[2],(names,names_w)),
all_=names+names_w
test=all_

def delete_libs(names=test):
    it=iter([names] if isinstance(names,str) else names)
    try:
        while True:
            delattr(a,next(it))
    except StopIteration:
        pass
    
   
def iter_libs(names=test):
    it=iter([names] if isinstance(names,str) else names)
    while True:
        try:
            yield getattr(a,next(it))
        except StopIteration:
            return

def _test(names=test,delete=True):
    if delete:
        delete_libs(names)
    # libs = *iter_libs(names),

    for l in iter_libs(names):
        # print(dict(l.metadata))
        print(l,'\n')
        print('\t list_symbols',l.list_symbols())
        print('\t metadata', l.metadata)
        print('\t custom_data', l.custom_data)

        print('\t get_name()', l.get_name())
        print('\t stats()', list(l.stats()))
        print('\t has_symbol(sym)', l.has_symbol(sym))
        
        lsym=l.get_symbol(sym)
        print('\t l.get_symbol(sym)', lsym)
        print('\t lsym.has_symbol()', lsym.has_symbol())
        print('')
        
def test_all(delete=True):
    _test(all_,delete)
    
def test_direct(delete=True):
    _test(names)
def test_wrappers(delete=True):
    _test(names_w)