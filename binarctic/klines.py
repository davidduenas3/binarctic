#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 23:11:34 2020

@author: ddd
"""
import enum
import functools as ft
import pandas as pd
class KInterval(str,enum.Enum):
    KLINE_INTERVAL_1MINUTE = '1m'
    KLINE_INTERVAL_3MINUTE = '3m'
    KLINE_INTERVAL_5MINUTE = '5m'
    KLINE_INTERVAL_15MINUTE = '15m'
    KLINE_INTERVAL_30MINUTE = '30m'
    KLINE_INTERVAL_1HOUR = '1h'
    KLINE_INTERVAL_2HOUR = '2h'
    KLINE_INTERVAL_4HOUR = '4h'
    KLINE_INTERVAL_6HOUR = '6h'
    KLINE_INTERVAL_8HOUR = '8h'
    KLINE_INTERVAL_12HOUR = '12h'
    KLINE_INTERVAL_1DAY = '1d'
    KLINE_INTERVAL_3DAY = '3d'
    KLINE_INTERVAL_1WEEK = '1w'
    KLINE_INTERVAL_1MONTH = '1M'

    __str__=lambda self: str.__str__(self)



def slot_get(instance, name):
    """
    Emulate ``instance.name`` using slot lookup as used for special methods
    This invokes the descriptor protocol, i.e. it calls the attribute's
    ``__get__`` if available.
    """

    owner = type(instance)
    attribute = getattr(owner, name)
    try:
        descriptor_get = attribute.__get__
    except AttributeError:
        return attribute
    else:
        return descriptor_get(instance, owner)
    
    
class L:
    def __init__(self,iterable):
        self._list=list(iterable)
        self.item=slot_get(self._list,'__getitem__')
        self.setitem=slot_get(self._list,'__setitem__')
    