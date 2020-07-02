#
import functools as ft
from arctic import arctic,date
from arctic.date import DateRange
from .klines import KlinesLib,KlinesLibWrapper
from . import chunker
# class DateRange(date.DateRange):
#     pass

class Arctic(arctic.Arctic):
    HOST='mongodb://arctic:arctic@localhost/arctic'
    def __init__(self,host=None,**kwargs):
        host=host or self.HOST
        super().__init__(host,**kwargs)

    klines_1h = KlinesLib.factory('klines_1h',interval='1h',chunker=chunker.year_chunker.TYPE)
    klines_1m = KlinesLib.factory('klines_1m',interval='1m',chunker=chunker.week_chunker.TYPE)
    # klines_1m2 = KlinesLib.factory('klines_1m2',interval='1m')

    klines_1h2=KlinesLibWrapper.factory('klines_1h2',interval='1h',chunker=chunker.year_chunker.TYPE)
        
 


    