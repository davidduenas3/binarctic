#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 20 18:38:16 2020

@author: ddd
"""
# from itertools import chain,slice
# import itertools as itt
from itertools import chain,islice
from collections import abc


__all__=['chunked','achunked','alist','aiter','anext','achain','aenumerate',
         'aislice',]




async def achain(*iterables):
    for iterable in iterables:
        async for i in aiter(iterable):
            yield i


async def aislice(aiterable, *args):
    '''
        aislice(stop)
        aislice(start, stop[, step])
    '''
    s = slice(*args)
    it = iter(range(s.start or 0, s.stop or sys.maxsize, s.step or 1))
    try:
        nexti = next(it)
    except StopIteration:
        return
    async for i, element in aenumerate(aiterable):
        if i == nexti:
            yield element
            try:
                nexti = next(it)
            except StopIteration:
                return
            
async def alist(iterable):
    return [i async for i in aiter(iterable)]
async def aset(iterable):
    return {i async for i in aiter(iterable)}
async def atuple(iterable):
    return tuple(await alist(iterable))

async def aenumerate(iterable):
    i = 0
    async for j in aiter(iterable):
        yield i, j
        i += 1
        
def chunked(iterable, n):
    iterator=iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, n-1))



async def achunked(iterable, n):
    aiterator=aiter(iterable)
    async for first in aiterator:
        yield achain([first],aislice(aiterator,n-1))
        
def _chunked_to_list(iterable, n):
    return (list(c) for c in chunked(iterable,n))

chunked.to_list=_chunked_to_list
def _achunked_to_list(iterable, n):
    return (await alist(c) async for c in achunked(iterable,n))

achunked.to_list=_achunked_to_list


    

class aiter(abc.AsyncIterable):
    
    def __new__(cls,iterable):
        if isinstance(iterable,abc.AsyncIterator):
            return iterable
        elif isinstance(iterable,abc.AsyncIterable):
            return iterable.__aiter__()
        elif isinstance(iterable,abc.Iterable):
            self=super().__new__(cls)
            self.iterator=iter(iterable)
            return self
        else:
            raise TypeError('%s debe ser iterable')
            
            
        
    def __repr__(self):
        return f'{type(self).__name__}({self.iterator})'
    
    def __aiter__(self):
        return self
    
    async def __anext__(self):
        try:
            return next(self.iterator)
        except StopIteration:
            raise StopAsyncIteration
            
aiter.__name__='AsyncIteratorWrapper'            
anext = lambda aiterator : aiterator.__anext__()

async def amap(func,*aiterables):
    aiterators = [aiter(i) for i in aiterables]
    while True:
        try:
            yield func(*[await anext(ai) for ai in aiterators])
        except StopAsyncIteration:
            break
        # yield func(*args)

# ii=aitt.islice(ii, 10)

# async def a():

        
if __name__=='__main__':
    
    ii=aiter(range(100))
    cc=achunked(ii,10)
    ll=(await atuple(i async for i in c) async for c in cc )
    
    # async for c in cc:
    #     print(await alist(c))
