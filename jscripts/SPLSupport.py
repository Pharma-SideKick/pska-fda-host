import json
from flask import Flask
import sqlalchemy as sa

def checkNone(item):
    if item is not None:
        return item[0]
    else:
        return ''

def openfile():
    database = DB()
    
    with open('DataParse/drug-label-0001-of-0012.json') as f:
        data = json.load(f)
        #print(type(data['results'][0].get('openfda').get('brand_name')[0]))
        for SPL in data['results']:
            tempOpenFDA = SPL.get('openfda')
            if tempOpenFDA is not None:
                #print(tempOpenFDA.get('generic_name'))
                database.insert_SPL(
                    brand_name=checkNone(tempOpenFDA.get('brand_name')),
                    generic_name=checkNone(tempOpenFDA.get('generic_name')),
                    when_using=checkNone(SPL.get('when_using')),
                    do_not_us=checkNone(SPL.get('do_not_use')),
                    stop_us=checkNone(SPL.get('stop_use')),
                    adverse_reactions=checkNone(SPL.get('adverse_reactions')),
                    drug_iteractions=checkNone(SPL.get('drug_iteractions')),
                )
    database.close()

def testDB():
    database = DB()
    database.select_med('Kroger Effervescent Antacid and Pain Relief')

class DB:
    def __init__(self):
        engine = sa.create_engine('sqlite:///SPL.db')
        self.connection = engine.connect()

        metadata = sa.MetaData()

        self.SPL_table = sa.Table(
            'SPL',
            metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('brand_name', sa.String),
            sa.Column('generic_name', sa.String),
            sa.Column('when_using', sa.String),
            sa.Column('do_not_us', sa.String),
            sa.Column('stop_us', sa.String),
            sa.Column('adverse_reactions', sa.String),
            sa.Column('drug_iteractions', sa.String),
        )
        metadata.create_all(engine)

    def insert_SPL(self,brand_name,generic_name,when_using,do_not_us,stop_us,adverse_reactions,drug_iteractions):
        query = self.SPL_table.insert().values(brand_name=brand_name,generic_name=generic_name,when_using=when_using,
                                        do_not_us=do_not_us,stop_us=stop_us,adverse_reactions=adverse_reactions,drug_iteractions=drug_iteractions)
        self.connection.execute(query)

    def select_med(self,brand_name):
        query = self.SPL_table.select().where(self.SPL_table.c.brand_name == brand_name)
        result = self.connection.execute(query)
        return result

    def close(self):
        self.connection.commit()
        self.connection.close()