#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  6 16:53:39 2020

@author: ddd
"""

import itertools as itt
import functools as ft
import types
from bson import SON
from getpass import getpass
from inspect import signature, Signature,Parameter,_ParameterKind as Kind ,_empty
from collections import namedtuple,Mapping,ChainMap


from pymongo import mongo_client,database,collection
from pymongo.errors import OperationFailure

from arctic import Arctic

from arctic.auth import Credential


class partial_cmd:
    _apply=staticmethod(lambda ins,ret:ret)
    
    def __init__(self,method,command,*,apply=None,**required_kw):
        self.method=method
        self.command=command
        self.params={k:Parameter(k,kind=Kind.KEYWORD_ONLY,default=v) for k,v in required_kw.items()}
        if apply:
            self.append_apply_fn(apply)
            
    def __get__(self,ins,owner=None):
        if ins is None:
            return self

        fn = ft.partial(self.method.__get__(ins),self.command)
        sig = signature(fn)
        
        wr_sig=sig.replace(parameters=sorted(
            ChainMap(sig.parameters,self.params).values(),
            key=lambda p:p.kind))
        
        
        @ft.wraps(fn)
        def wrapped(*args,**kwargs):
            ba=wr_sig.bind(*args,**kwargs)
            ba.apply_defaults()
            ret = fn(*ba.args,**ba.kwargs)
            return self._apply(ins,ret)
        
        wrapped.__signature__ = wr_sig
        return wrapped
    
    def append_apply_fn(self,fn):
        assert(len(signature(fn).parameters)==2)
        last=self._apply
        self._apply=lambda ins,ret:fn(ins,last(ins,ret))
        return self
            

        

    
    
class MongoClient(mongo_client.MongoClient):
    Role = property(lambda self:ft.partial(Role,self))
    User = property(lambda self:ft.partial(User,self))


class Database(database.Database):
    
    
    Role = property(lambda self:ft.partial(Role,self.client,db=self.name))
    User = property(lambda self:ft.partial(User,self.client,db=self.name))
    
    def command(self,command,value=1,*args,**kwargs):
        s=signature(super().command)
        ba=s.bind(command,value,*args,**kwargs)
        return super().command(*ba.args,**ba.kwargs)
    
    _cmd=ft.partial(partial_cmd,command)
    
    rolesInfo=_cmd('rolesInfo',
                   apply=lambda ins,ret:ret['roles'],
                   showPrivileges=False,
                   showBuiltinRoles=False)                            
    
    createRole =_cmd('createRole', roles=(), privileges=())
    dropRole= _cmd('dropRole')
    grantRolesToRole = _cmd('grantRolesToRole',roles=_empty)
    
    usersInfo=_cmd('usersInfo',
                   apply=lambda ins,ret:ret['users'],
                   showCredentials=False,
                   showPrivileges=False)
    
    dropUser=_cmd('dropUser')
    grantRolesToUser = _cmd('grantRolesToUser',roles=_empty)
    createUser =_cmd('createUser',pwd=_empty, roles=())
    
    
database.Database.__signature__=signature(database.Database)                           
database.Database.__new__ = lambda cls,*a,**ka:object.__new__(Database)

class _CommonRU():
    def __init__(self,conn,/,*args,**kwarg):
        self.conn=conn
        if kwarg.get('check') and not self.exists():
            raise ValueError('%s not exists' % str(self))
        if kwarg.get('authenticate'):
            assert self.authenticate()
            
    @property
    def _db(self):
        return self.conn[self.db]
    
    @property
    def command(self):
        return ft.partial(self._db.command,value=self[0])    
    
    def getInfo(self):
        return self._getInfo.__get__(self._db)(self[0])[0]
    
    def exists(self):
        try:
            self.getInfo();return True
        except IndexError:
            return False

    
    def getRoles(self):
        return [self._db.Role(**r) for r in self.getInfo()['roles']]
        
    def grantRoles(self,roles=_empty):
        assert self.exists() 
        def formatRole(r):
            if isinstance(r,Role):
                assert r.exists() ,'%s not exists' % str(r)
                return r._asdict()
            if isinstance(r,Mapping):
                return formatRole(self.conn.Role(**r))
            raise TypeError('%s not supported' % type(r))
            
        roles=[formatRole(r) for r in roles]
        fn=self._grantRoles.__get__(self._db)(self[0],roles=roles)
        return fn
    
    def grantRole(self,role):
        return self.grantRoles([role])
    
    def drop(self):
        return self._drop.__get__(self._db)(self[0])
    
    @property
    def create(self):
        return ft.partial(self._create.__get__(self._db),self[0])

nt_Role=namedtuple('nt_Role','role db')
class Role(nt_Role,_CommonRU):
    _getInfo=staticmethod(Database.rolesInfo)
    _grantRoles=staticmethod(Database.grantRolesToRole)
    _create=staticmethod(Database.createRole)
    _drop=staticmethod(Database.dropRole)
    def __new__(cls,conn,/,role,db,*,check=False,**kwarg):
        self = super().__new__(cls,role=role,db=db)
        return self
    

    
nt_User=namedtuple('nt_User','user db')
class User(nt_User,_CommonRU):
    '''
    '''
    _getInfo=staticmethod(Database.usersInfo)
    _grantRoles=staticmethod(Database.grantRolesToUser)
    _create=staticmethod(Database.createUser)
    _drop=staticmethod(Database.dropUser)
    
    def __new__(cls,conn,/,user,db,pwd=None,*,check=False, authenticate=False,**kwarg):
        self = super().__new__(cls,user=user,db=db)
        self._pwd=pwd
        return self
    
    @property
    def pwd(self):
        return self._pwd or input('pwd (%s)? ' % self.user)
    
    def authenticate(self,pwd=None):
        pwd = pwd or self.pwd
        try:
            if self.conn[self.db].authenticate(self.user,pwd):
                self._pwd=pwd
                return True
        except OperationFailure:
            return False
        except:
            raise
      
cl=MongoClient()
db=cl.arctic
u=db.User('arctic',authenticate=True)

a=Arctic(cl)






