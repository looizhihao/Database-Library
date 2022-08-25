from abc import ABC,abstractmethod #Abstract Base Class
from dataclasses import dataclass, field

from asyncio.log import logger

import pandas as pd


# This is for interaction with DB
import psycopg2
import pyodbc
import sqlite3 as sql
from sqlalchemy.engine import URL
from sqlalchemy import create_engine


@dataclass
class Database(ABC):
    database:str
    conn = None  
    def __enter__(self):
        self.conn = self.connect()
        return self 

    def __exit__(self, exc_type, exc_val, exc_tb):
        try: 
            if exc_tb is None:
                self.commit()
            else:
                self.rollback()
        except:
            pass
        finally:
            self.close()

    def commit(self):
        self.conn.commit()
    def rollback(self):
        self.conn.rollback()
    def close(self):
        self.conn.close()

    def cursor(self):
        return self.conn.cursor()    
    def list_attrs(self,tablename) :
        print ('Function not supported')
    def list_tablenames(self,tablename) :
        print ('Function not supported')
    def max(self,tablename, attr) :
        print ('Function not supported')
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass


  
@dataclass
class sqliteDB(Database):
    pk:str = None
    def __str__(self) -> str:
        return f"Sqlite db {self.database}"

    def connect(self):
        self.conn=sql.connect(self.database)
        return self.conn

    def list_attrs(self,tablename) :
        with self.connect() as db:
            select = 'PragMA table_info([{tn}])'.format(tn=tablename)
            where = ''
            cmd = ' '.join([select, where])
            logger.debug(cmd)

            dd = pd.read_sql_query(cmd, db)
        return list(dd['name'])
    def max_id(self, tablename, attr):
        return f'SELECT MAX({attr}) + 1 FROM [{tablename}]'

    def list_tablenames(self):    
        with self.connect() as db:
            select = 'SELECT name FROM "{tn}"'.format(tn="sqlite_master")
            where = 'WHERE type="table";'
            cmd = ' '.join([select, where])
            logger.debug(cmd)

            dd = pd.read_sql_query(cmd, db)
        return list(dd['name'])
        
@dataclass
class securedDB(Database):
    host: str = field(repr=False,default=None)
    user: str  = field(repr=False,default='Trusted_Connection')
    _password: str = field(repr=False,default=None)
    port: int  = field(repr=False,default=1433)
    pk:str = field(default=None)


@dataclass
class postgresDB (securedDB): #not tested class
    
    def __str__(self) -> str:
        return f"postgresql {self.database} connected using {self.user}@{self.host}"

    def connect(self):
        self._conneciton= psycopg2.connect(host=self.host,
                                database=self.database,
                                user=self.user,
                                password=self._password)
        return self._conneciton

@dataclass
class pyodbcDB (securedDB): #not tested class

    def __str__(self) -> str:
        return f"odbc {self.database} connected using {self.user}@{self.host}"

    def connect(self):
        cnxn_str = (f"Driver={{ODBC Driver 18 for SQL Server}};"
                    f"Server={self.host};"
                    f"Database={self.database};"
                    "Encrypt=no;"
                    f"UID={self.user};"
                    f"PWD={self._password};"
                    )
        self.conn=pyodbc.connect(cnxn_str)
        return self.conn

@dataclass
class alchmeyDB (securedDB):

    def __str__(self) -> str:
        return f"SQL Alchemy {self.database} connected using {self.user}@{self.host}"

    def connect(self):
        cnxn_str = (f"Driver={{ODBC Driver 18 for SQL Server}};"
                    f"Server={self.host},{self.port};"
                    f"Database={self.database};"
                    "Encrypt=no;")
        
        if self.user == 'Trusted_Connection' :
            cnxn_str+="Trusted_Connection=yes;"
        else :
            cnxn_str+=f"UID={self.user};"
            if self._password != None:    
                cnxn_str+=f"PWD={self._password};"
                    
        cnxn_url =URL.create("mssql+pyodbc", query={"odbc_connect": cnxn_str})
        self._engine=create_engine(cnxn_url)
        self.conn=self._engine.connect()
        return self.conn
        
    def cursor(self):
        return self.conn
    
    def commit(self):     
        return 
    
    def rollback(self):
        return 

    def list_attrs(self,tablename) :
        with self.connect() as db:
            select = 'SELECT COLUMN_NAME From INFORMATION_SCHEMA.COLUMNS'
            where = f'WHERE TABLE_NAME=\'{tablename}\''
            cmd = ' '.join([select, where])
            logger.debug(cmd)

            dd = pd.read_sql_query(cmd, db)
            print (dd)
        return list(dd['COLUMN_NAME'])

    def max_id(self, tablename, attr):  
        return f'SELECT ISNULL(MAX({attr}) + 1, 1) FROM [{tablename}]'

    def list_tablenames(self):    
        with self.connect() as db:
            cmd = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;'
            logger.debug(cmd)
            dd = pd.read_sql_query(cmd, db)
        return list(dd['TABLE_NAME'])


if __name__ == "__main__":
    import sys

    hostname="192.168.3.54" 
    port=1433
    username='IAmAUser'
    password='Str0ngPasswordIsNotVeryLong'
    db_name='database name'

    
    
    #check for drivers available
    driver_names = [x for x in pyodbc.drivers() if x.endswith(' for SQL Server')]    
    print ("Avaiable Drivers: ",driver_names)
    
    #check for server port opened
    import socket 
    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    location = (hostname, port)
    print ("Testing Socket :",location)

    def exit():
        
        print ("Server Port Open FAILED")
        sys.exit()
    
    print ("Server port opened !!!" if a_socket.connect_ex(location) ==0 else exit())
    
    
    print ("Testing db connection")
    #instantiate 
    # username,password and port are optional 
    # will use trusted connection if username and password not provided
    mydb = alchmeyDB(db_name,hostname,username,password,port)
 
    print(mydb)
    #get list of table names
    all_tables=mydb.list_tablenames()
    print("Tables Available:",*all_tables,sep='\n')
    


    #write to db
    test_table_name='test_create_table' 
    if test_table_name in all_tables:
        print('test_create_table exist')
        sys.exit()
    with mydb as db:
        cs = db.cursor()
        mesql=f'create table [{test_table_name}](id varchar(64),name varchar(64),age varchar(64)); '
        try:
            cs.execute(mesql)
            print ('create table success')
            mesql=f"insert into [{test_table_name}] values (1,'alex' ,20),(2,'paul',30);"
            cs.execute(mesql)
            print ('done insert data')
            
        except:
            print('permission not granted')
    #read from db
    with mydb as db:
        mesql=f'SELECT * FROM [{test_table_name}]'
        dd = pd.read_sql_query(mesql, db.conn)
        print('All')
        print (dd)
        mesql=f'SELECT * FROM [{test_table_name}] where [name] in (?)'
        param=['alex']
        print('look for alex')
        dd = pd.read_sql_query(mesql, db.conn,params=param)
        print (dd)

    #remove table from db        
    with mydb as db:
        cs = db.cursor()
        mesql='drop table [test_create_table]'
        try:
            cs.execute(mesql)
            print ('done remove test_create_table')
        except:
            pass
    all_tables=mydb.list_tablenames()
    print("Remainint tables:",*all_tables,sep='\n')


    #or you can use the style below
    mydb.connect()  
    cs = mydb.cursor()
    mesql=f'create table [{test_table_name}](id varchar(64),name varchar(64),age varchar(64)); '
    cs.execute(mesql)
    
    mesql=f"insert into [{test_table_name}] values (1,'alex.g' ,20),(2,'paul.g',30);"
    cs.execute(mesql)
    
    mesql=f'SELECT * FROM [{test_table_name}]'
    dd = pd.read_sql_query(mesql, db.conn)
    print (dd)
    
    mesql='drop table [test_create_table]'
    cs.execute(mesql)
