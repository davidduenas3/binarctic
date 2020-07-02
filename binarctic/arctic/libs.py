#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 18 22:48:13 2020

@author: ddd
"""

import functools as ft
from types import new_class,MethodType
from arctic import arctic,exceptions,chunkstore,tickstore,store,register_library_type
from collections.abc import MutableMapping,Mapping,MutableSequence

class LibFactory(object):
    def __init__(self,name,lib_type,**kwargs):
        self.name=name
        self.lib_type=lib_type #if isinstance(lib_type,str) else lib_type.lib_type
        self.initkw=kwargs
    
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        return self._get(ins)
    
    def __delete__(self,db):
        db.delete_library(self.name)
        
    def _get(self,db):
        try:
            return db.get_library(self.name)
        except arctic.LibraryNotFoundException:
            db.initialize_library(self.name,self.lib_type,**self.initkw)
            return self._get(db)
    
    @classmethod
    def register_type(_cls,lib_type):
        def _wrapper(cls):
            try:
                arctic.register_library_type(lib_type,cls)
                cls.lib_type=lib_type
                cls.factory=staticmethod(lambda name,**initkw:_cls(name,lib_type,**initkw))
                return cls
            except exceptions.ArcticException as e:
                raise e
                # print(e)
            
        
        return _wrapper
    

    
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

class LibSymbol(object):
    def __init__(self,lib,symbol):
        self.lib=lib
        self.symbol=symbol
        
class descriptorSymbol(object):
    def __init__(self,cls):
        self.symbol_class=cls
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        return types.MethodType(self.symbol_class,ins)
    
    


class LibMeta(type):

    def __prepare__(mtc,name,bases,**kwargs):
        
   
    class metadata(Mapping):
        def __init__(self,lib):
            self.lib=lib
            self._keys = lib._arctic_lib.library_metadata_keys
            self._getitem = lib._arctic_lib.get_library_metadata
            self._setitem = lib._arctic_lib.set_library_metadata
        
        def __len__(self):
            return len(self._keys())
        
        def __iter__(self):
            return iter(self._keys())
        def __repr__(self):
            return "{} <{}>".format(type(self).__name__,self.lib._arctic_lib.get_name())
        
        def __getitem__(self,key):
            if not key in self._keys():
                raise KeyError(key)
            value = self._getitem(key)
            return PropagatedItem(self,key,value)

        
        def __delitem__(self,key):
            raise AttributeError('__delitem__')
            
        def __setitem__(self,key,value):
            self._setitem(key,value)
            
        
        @classmethod
        class descriptor(object):
            def __init__(self,mdataclass):
                ft.wraps(mdataclass)(self)
                
            def __get__(self,instance,owner=None):
                if instance is None:
                    return self
                return self.__wrapped__(instance)




        

CUSTOM_METADATA='CUSTOM_METADATA'

class KeyProperty(object):
    def __init__(self,desc, key, default=None, 
                 allow_set=True, allow_delete=True,
                 getter=None):
        self.desc=desc
        self.key=key
        self.default = default if callable(default) or default is None else lambda s:default
        self.allow_set=allow_set
        self.allow_delete=allow_delete
        self.getter=getter
        
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        map=self.desc.__get__(ins,owner)
        if not self.key in map and not self.default is None:
            map[self.key]=self.default(ins)
        value = map[self.key]
        return value if self.getter is None else self.getter(ins,value)
    
    def __set__(self,ins,value):
        if not self.allow_set:
            raise AttributeError(self.key)
        map=self.desc.__get__(ins)
        map[self.key] = value 
        
    def __delete__(self,ins):
        if not self.allow_delete:
            raise AttributeError(self.key)
        map=self.desc.__get__(ins)
        del map[self.key]
    
    def _defaultsetter(self,fn):
        kw={**vars(self),'default':fn}
        return type(self)(**kw)
    
    def _getterfn(self,fn):
        kw={**vars(self),'getter':fn}
        return type(self)(**kw)
        
   
    @classmethod
    def Default(cls,desc,key,**kwargs):
        return cls(desc,key,**kwargs)._defaultsetter

class Lib_Arctic(metaclass=LibMeta):
    
    metadata=LibMeta.metadata.descriptor()
    
    custom_data=KeyProperty(metadata,CUSTOM_METADATA,dict(),allow_set=False,allow_delete=False)
    

    @classmethod
    def initialize_library(cls,arctic_lib,custom_data=(),**kwargs):
        super().initialize_library(arctic_lib, **kwargs)
        arctic_lib.set_library_metadata(CUSTOM_METADATA,dict(custom_data))
        # cls.library_metadata.__wrapped__(arctic_lib)[CUSTOM_METADATA]=dict(custom_data)
        
    
    

        
        
MCHUNKER = 'CHUNKER'

@LibFactory.register_type("ChunkStoreV4")
class ChunkStore(Lib_Arctic,chunkstore.ChunkStore):
    
    @classmethod
    def initialize_library(cls,arctic_lib, chunker=chunkstore.DateChunker.TYPE,**kwargs):
        super().initialize_library(arctic_lib, **kwargs)
        arctic_lib.set_library_metadata(MCHUNKER,chunker)
        
        # return cls.lib_class.initialize_library(arctic_lib,**kwargs)
    
    @property
    def chunker(self):
        name = self._arctic_lib.get_library_metadata(MCHUNKER)
        return chunkstore.get_chunker(name)
    
    @property
    def write(self):
        return ft.partial(super().write,chunker=self.chunker)

@LibFactory.register_type("TickStoreV4")
class TickStore(Lib_Arctic,tickstore.tickstore.TickStore):
    pass

@LibFactory.register_type("VersionStoreV4")
class VersionStore(Lib_Arctic,arctic.version_store.VersionStore):
    pass

 





class wrapped_libmethod(object):
    def __init__(self,name):
        self.name=name
    def __get__(self,ins,owner=None):
        if ins is None:
            return getattr(owner.lib_class,self.name).__get__(None,owner.lib_class)
        return getattr(type(ins).lib_class,self.name).__get__(ins.lib,type(ins).lib_class)
    
    
class wrapped_attribute(wrapped_libmethod):
    def __get__(self,ins,owner=None):
        if ins is None:
            return self
        return getattr(ins.lib,self.name)
    def __set__(self,ins,value):
        return setattr(ins.lib,self.name,value)
    def __delete__(self,ins):
        return delattr(ins.lib,self.name)

class LibWrapperMeta(type):
    __lib_class=None
    @property
    def lib_class(cls):
        return cls.__lib_class
    
    @classmethod
    def __prepare__(mtc,name,bases,**kwargs): 
        attrs=super().__prepare__(name,bases)
        if not any(map(lambda x:isinstance(x,mtc),bases)):
            attrs.update(
                lib=property(lambda self:self.__lib),
                __wrapped__=property(lambda self:self.__lib),
                metadata = wrapped_libmethod('metadata')
                )
        return attrs
    
    def __new__(mtc,name,bases,attrs,lib_class=None,**kwargs):
        lib_class=lib_class or attrs.pop('__lib_class__',None)
        cls=super().__new__(mtc,name,bases,attrs,**kwargs)
        if lib_class:
            cls.__lib_class=lib_class
        # if cls.lib_class:
        #     cls.wrap_name('_arctic_lib')
        return cls
    
    @property
    def initialize_library(kls):
        if 'initialize_library' in kls.__dict__:
            return kls.__dict__['initialize_library'].__get__(None,kls)
        else:
            return kls.lib_class.initialize_library

    
    def __call__(cls,arctic_lib, **kwargs):
        if cls.lib_class:
            self = super().__call__()
            self.__lib=cls.lib_class(arctic_lib, **kwargs)
            return self
        raise TypeError('lib_class??')
    
    def create_wrapper(cls,lib_class):
        name=lib_class.__name__ + "Wrapper"
        bases=(cls,)
        return new_class(name,bases,dict(lib_class=lib_class))
    
    def wrap_name(cls,name,alias=None):
        alias=alias or name
        if alias in cls.__dict__:
            raise AttributeError(alias + ' ya existe')
        setattr(cls,alias,cls.get_wrapper(name))

    def get_wrapper(cls,name):
        if hasattr(cls.lib_class,name):
            a=getattr(cls.lib_class,name)
            import inspect
            if inspect.isdatadescriptor(a):
                return wrapped_attribute(name)
            if hasattr(a, "__get__") and not hasattr(a, "__set__"):
                return wrapped_libmethod(name)
            raise AttributeError(type(a))
        else:
            return wrapped_attribute(name)
            

            
class LibWrapper(metaclass=LibWrapperMeta):
    __repr__ = wrapped_libmethod('__repr__')
    # property(lambda self:self.lib.__repr__)
    _arctic_lib = wrapped_attribute('_arctic_lib')
    # _arctic_lib = property(lambda self:self.lib._arctic_lib)
    stats = property(lambda self:self.lib.stats)
    
    # @classmethod
    # def initialize_library(kls,*args,**kwargs):
    #     kls.lib_class.initialize_library(*args,**kwargs)    
    
    
# class LibChunkStore(LibWrapper,lib_class=ChunkStore):
#     pass   
# class LibTickStore(LibWrapper,lib_class=tickstore.tickstore.TickStore):
#     pass
# class LibVersionStore(LibWrapper,lib_class=arctic.version_store.VersionStore):
#     pass

# __all__=['LibFactory','LibChunkStore','LibTickStore','LibVersionStore']

