import requests
import json
import psycopg2
from dotenv import load_dotenv   #for python-dotenv method
#load_dotenv()                    #for python-dotenv method
import os 
from datetime import datetime


env_path="/d/users/jvicenth/OneDrive - Rimac Seguros y Reaseguros/CURSOS/AUTOMATIZACION/bvl/.env"
load_dotenv(env_path)

#SOLO VALORES
#https://dataondemand.bvl.com.pe/v1/stock-quote/share-values/BAP?startDate=2010-05-21&endDate=2021-05-21
#MAS DETALLE
#https://dataondemand.bvl.com.pe/v1/issuers/stock/FERREYC1?startDate=2009-04-22&endDate=2021-05-22
URL_HISTORICO = "https://dataondemand.bvl.com.pe/v1/issuers/stock/{}?startDate={}&endDate={}"
URL = "https://dataondemand.bvl.com.pe/v1/stock-quote/home"
var_payload = {"sector": "", "isToday": "True", "companyCode": "", "inputCompany": ""}

#db_host = os.environ.get('HOST')
#db_database = os.environ.get('DATABASE')
#db_user = os.environ.get('USER')
#db_port = os.environ.get('PORT')
#db_password = os.environ.get('PASSWORD')
#db_uri = os.environ.get('URI')
db_host = "ec2-3-233-7-12.compute-1.amazonaws.com"
db_database = ""
db_user = ""
db_port = 5432
db_password = ""

def connect_postgres():
    print(db_host)
    conn = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password)
    return conn

def select_companyStock():
    query = """
    SELECT DISTINCT companyCode,companyName,nemonico,sectorCode,sectorDescription
    FROM companyStock
    """

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[2])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode

def select_stockHistory(nemonico):
    query = """
    SELECT DISTINCT id  ,nemonico ,date ,open ,close ,high ,low ,average ,quantityNegotiated ,solAmountNegotiated,
           dolarAmountNegotiated ,yesterday ,yesterdayClose ,currencySymbol 
    FROM stockHistory
    WHERE nemonico='{}'
    ORDER BY date DESC
    LIMIT 1
    """.format(nemonico)
    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[2])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode


def insert_row_companyStock(row):
    try:
        if  "sectorCode" not in row:
            row["sectorCode"] = "EXT"
            row["sectorDescription"] = "EXTRANJERO"

        query = """INSERT INTO companyStock (companyCode,companyName,nemonico,sectorCode,sectorDescription) 
                VALUES ('{}','{}','{}','{}','{}')""".format(row["companyCode"],row["companyName"],row["nemonico"],row["sectorCode"],row["sectorDescription"])
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     

def insert_row_stockHistory(lst_row):
    try:
        
        query = """INSERT INTO stockHistory (id,nemonico,date,open,close,high,low,average,quantityNegotiated,solAmountNegotiated,dolarAmountNegotiated,yesterday,yesterdayClose,currencySymbol)
                VALUES {} """.format(lst_row)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE companyStock (
            companyCode VARCHAR(255) NOT NULL,
            companyName VARCHAR(255) NULL,
            nemonico VARCHAR(255) NULL,
            sectorCode VARCHAR(255) NULL,
            sectorDescription VARCHAR(255) NULL
        )
        """,
        """ CREATE TABLE stockHistory (
                id  VARCHAR(255) NOT NULL,
                nemonico VARCHAR(255) NOT NULL,
                date VARCHAR(255) NOT NULL,
                open VARCHAR(255) NOT NULL,
                close VARCHAR(255) NOT NULL,
                high VARCHAR(255) NOT NULL,
                low VARCHAR(255) NOT NULL,
                average VARCHAR(255) NOT NULL,
                quantityNegotiated VARCHAR(255) NOT NULL,
                solAmountNegotiated VARCHAR(255) NOT NULL,
                dolarAmountNegotiated VARCHAR(255) NOT NULL,
                yesterday VARCHAR(255) NOT NULL,
                yesterdayClose VARCHAR(255) NOT NULL,
                currencySymbol VARCHAR(255) NOT NULL
                )
        """)

    # connect to the PostgreSQL server
    conn = connect_postgres()
    cur = conn.cursor()
    # create table one by one
    for command in commands:
        cur.execute(command)
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    conn.commit()

def get_stock_list():
    r = requests.post(URL, data=json.dumps(var_payload))
    lista_codigos = json.loads(r.text)
    return lista_codigos

def get_stock_list_values(nemonico, startDate, endDate):
    r = requests.get(URL_HISTORICO.format(nemonico,startDate,endDate))
    lista_values = json.loads(r.text)
    return lista_values


if __name__ == "__main__":
    #create_tables() -- SOLO EJECUTAR 1 VEZ
    # Obtengo la info de la web
    stock_list = get_stock_list()
    print(stock_list)
    # Obtengo la lista de codigos grabado en la BD
    listStockCode = select_companyStock()
    # Inserto todos los row que faltaban.
    entro = False
    for i in stock_list:
        if i["nemonico"] not in listStockCode:
            #print(i)
            insert_row_companyStock(i)
            entro = True
    if entro:
        #obtengo la lista actualizada
        listStockCode = select_companyStock()

    # Registro la data historica
    for i in listStockCode:
        nemonico = i
        print(nemonico)
        lsh = select_stockHistory(nemonico)
        print(lsh)
        if len(lsh) > 0:
            startDate = lsh[0]
        else:
            startDate = "2000-01-01"
        
        endDate = datetime.today().strftime('%Y-%m-%d')

        list_values = get_stock_list_values(nemonico, startDate, endDate)
        str_values = ""
        for val in list_values:
            try:
                if "id" not in val:
                    val["id"] = ""
                if "nemonico" not in val:
                    val["nemonico"] = ""
                if "date" not in val:
                    val["date"] = ""
                if "open" not in val:
                    val["open"] = ""
                if "close" not in val:
                    val["close"] = "" 
                if "high" not in val:
                    val["high"] = ""
                if "low" not in val:
                    val["low"] = ""
                if "average" not in val:
                    val["average"] = ""
                if "quantityNegotiated" not in val:
                    val["quantityNegotiated"] = ""
                if "solAmountNegotiated" not in val:
                    val["solAmountNegotiated"] = ""
                if "dollarAmountNegotiated" not in val:
                    val["dollarAmountNegotiated"]
                if "yesterday" not in val:
                    val["yesterday"] = ""
                if "yesterdayClose" not in val:
                    val["yesterdayClose"] = "" 
                if "currencySymbol" not in val:
                    val["currencySymbol"]

                str_values = str_values + "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(val["id"],val["nemonico"],val["date"],val["open"],val["close"],val["high"],val["low"],val["average"],val["quantityNegotiated"],val["solAmountNegotiated"],val["dollarAmountNegotiated"],val["yesterday"],val["yesterdayClose"],val["currencySymbol"])
            except Exception as e:
                print(e)
            
        str_values = str_values[:-1]
        if len(str_values)>0:
            insert_row_stockHistory(str_values)
        
