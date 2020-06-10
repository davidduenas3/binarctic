#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 20 18:38:16 2020

@author: ddd
"""
# from itertools import chain,slice
# import itertools as itt
import sys
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

# itt.chunked=chunked

async def achunked(iterable, n):
    aiterator=aiter(iterable)
    async for first in aiterator:
        yield achain([first],aislice(aiterator,n-1))
        


async def atakewhile(predicate,iterable):
    async for i in aiter(iterable):
        if not predicate(i):
            break
        yield i
        
async def adropwhile(predicate,iterable):
    aiterator=aiter(iterable)
    async for i in aiterator:
        if not predicate(i):
            yield i
            break
    async for i in aiterator:
        yield i
            


    

class aiter(abc.AsyncIterator):
    
    def __new__(cls,iterable):
        if isinstance(iterable,abc.AsyncIterator):
            return iterable
        elif isinstance(iterable,abc.AsyncIterable):
            return iterable.__aiter__()
        elif isinstance(iterable,abc.Iterator):
            self=super().__new__(cls)
            self.iterator=iterable
            return self
        elif isinstance(iterable,abc.Iterable):
            return aiter.__new__(cls,iter(iterable))
        else:
            raise TypeError('%s debe ser iterable' % iterable)
            
            
        
    def __repr__(self):
        return f'{type(self).__name__}({self.iterator})'
    
    def __aiter__(self):
        return self
    
    async def aclose(self):
        self.iterator.close()
    
    async def __anext__(self):
        try:
            return next(self.iterator)
        except StopIteration:
            raise StopAsyncIteration
            
aiter.__name__='AsyncIteratorWrapper'            


def anext(iterator):
    if isinstance(iterator,abc.AsyncIterator):
        return iterator.__anext__()
    elif isinstance(iterator,abc.Iterator):
        return aiter(iterator).__anext__()
    raise TypeError('%s debe ser iterator' % iterator)
    
async def amap(func,*aiterables):
    aiterators = [aiter(i) for i in aiterables]
    while True:
        try:
            yield func(*[await anext(ai) for ai in aiterators])
        except StopAsyncIteration:
            break
        
async def afilter(func,iterable):
    func = func or bool
    aiterator=aiter(iterable)
    async for i in aiterator:
        if func(i):
            yield i
    
        # yield func(*args)

# ii=aitt.islice(ii, 10)

# async def a():

        
if __name__=='__main__':
    
    # ii=aiter(range(100))
    cc=achunked(range(100),10)
    rr=aiter(range(100))
    rr=afilter(lambda x:x>12 and x<=33,range(88))
    # rr=atakewhile(lambda x:x<=33,adropwhile(lambda x:x<12,rr))
    ll=([i async for i in c] async for c in cc)
    
    # async for c in cc:
    #     print(await alist(c))
