#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 21:18:52 2020

@author: ddd
"""


from arctic import chunkstore



class DateChunker(chunkstore.DateChunker):
    TYPE=property(lambda self : self.name)
    
    def __init__(self, name, chunk_size):
        self.name = name
        self.chunk_size = chunk_size
    
    
    def to_chunks(self, df, chunk_size=None, func=None):
        assert chunk_size is None or chunk_size==self.chunk_size
        return super().to_chunks(df, chunk_size=self.chunk_size,func=func)



year_chunker = chunkstore.register_chunker(DateChunker('date_y', 'A'))
month_chunker = chunkstore.register_chunker(DateChunker('date_m', 'M'))
week_chunker = chunkstore.register_chunker(DateChunker('date_w', 'W'))
day_chunker = chunkstore.register_chunker(DateChunker('date_d', 'D'))
