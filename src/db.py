import os
from sqlalchemy import create_engine
import sqlalchemy as db
import pandas as pd
import datetime

class Sql_Alchemy:

    __DB__Name__ = os.getcwd() + '/databse/test.db'

    def ___init_(self):
        self.engine = create_engine(f'sqlite:///{Sql_Alchemy.__DB__Name__}')
        self.connection = self.engine.connect()

    def create_connection(self):
        self.engine = create_engine(f'sqlite:///{Sql_Alchemy.__DB__Name__}')
        self.connection = self.engine.connect()

    def create_table(self,table):
        if not db.inspect(self.engine).has_table(table):
            metadata= db.MetaData(self.engine)
            db.Table(table,metadata,
            db.Column('key',db.Text),
            db.Column('_id',db.Integer),
            db.Column('name',db.Text),
            db.Column('status',db.Boolean),
            db.Column('time',db.DateTime))
        metadata.create_all()

    def store_db_input(self,table,key,_id,name,status):
        stats_table = db.Table(table,db.MetaData(),autoload=True, autoload_with=self.engine)
        query = db.insert(stats_table).values(key=key,_id=_id,name=name,
                status=status,time=datetime.datetime.now())
        result= self.connection.execute(query)
        result.close()

    def read_data(self,table):
        metadata= db.MetaData(self.engine)
        table=db.Table(table,metadata,autoload=True, autoload_with=self.engine)
        query = db.select([table]).where(table.columns.status==True)
        df=pd.read_sql_query(query,self.connection)
        return df






if __name__ == '__main__':
    cdb=Sql_Alchemy()
    cdb.create_connection()
    cdb.create_table("census")
    cdb.store_db_input("census","data1","2","xxx",True)
    val=cdb.read_data('census')
    print(val)
