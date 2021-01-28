import asyncio,json,logging
import aiomysql 

def log(sql,args=()):
    logging.info('SQL: s%' % sql)

# 设置数据库资源连接池
@asyncio.coroutine
def create_pool(loop,**kw):
    logging.info("create database connection pool ...")
    global __pool
    __pool=yield from aiomysql.create_pool(
        host=kw.get("host","localhost"),
        port=kw.get("port",3306),
        user=kw["user"],
        password=kw["password"],
        db=kw["db"],
        charset=kw.get("charset","UTF-8"),
        autocommit=kw.get("autocommit",True),
        maxsize=kw.get("maxsize",10),
        minsize=kw.get("minsize",1),
        loop=loop
    )

# select查询函数封装
@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with(yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.excute(sql.replace('?','%s'),args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows return %s' % len(rs))
        return rs

# excute执行函数封装
@asyncio.coroutine
def excute(sql,args):
    log(sql)
    with(yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.excute(sql.replace('?','%s'),args)
            affected = cur.rowcount()
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

#定义基类Model
class Model(dict,metaclass=ModelMetaClass):
    def __init__(self, **kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    
    def __setattr__(self, key, value):
        self[key]=value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            field=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key,str(value)))
                setattr(self,key,value)
        return value

#Field字段类及子类
class Field(object):
    def __init__(self,name,column_type,primary_key,default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default
    
    def __str__(self):
        return '<%s,%s:%s>' % (self.__class__.__name__,self.column_type,self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None,ddl="varchar(100)"):
        super().__init__(name,ddl, primary_key, default)

#实现属性及数据库字段映射关系
class ModelMetaClass(type):
    def __new__(cls,name,bases,attrs):
        #排除基类Model
        if name == "Model" :
            return type.__new__(cls,name,bases,attrs)
        #获取表的名字
        tableName=attrs.get("__table__",None) or name
        logging.info("find model: %s(table: %s)" % (name,tableName))
        #获取所有的字段以及主键名称
        mappings=dict()
        fields=[]
        primaryKey=None
        for k,v in attrs.items():
            if isinstance(v,fields):
                logging.info("found mappings：%s => %s" % (k,v))
                fields[k]=v
                #寻找主键
                if v.primary_key:
                    if primaryKey:
                        raise("Duplicate primary key for field: %s" % k)
                    primaryKey=k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        
