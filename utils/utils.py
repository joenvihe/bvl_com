import requests
import json
import psycopg2
from dotenv import load_dotenv
import os 
from datetime import datetime
import yaml

def get_config():
    with open('./config/setting.yml', 'r') as file:
        setting = yaml.safe_load(file)
    return setting

def connect_postgres():
    setting = get_config()
    conn = psycopg2.connect(
    host = os.environ.get('HOST'),
    database =  os.environ.get('DATABASE'),
    user = os.environ.get('USER'),
    password = os.environ.get('PASSWORD')
    )
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
        list_stockCode.append(row[2])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode


def insert_row_stockistoday(lst_row):
    try:
        
        query = """INSERT INTO public.stockistoday(
	companycode, companyname, shortname, nemonico, sectorcode, sectordescription, lastdate, previousdate, buy, sell, previous, negotiatedquantity, negotiatedamount, negotiatednationalamount, operationsnumbe, currency, unity, segment, createddate)
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
    setting = get_config()
    var_payload = {"sector": "", "isToday": "True", "companyCode": "", "inputCompany": ""}
    r = requests.post(setting["url_bvl"]["url_lista_acciones"], data=json.dumps(var_payload))
    lista_codigos = json.loads(r.text)
    return lista_codigos

def get_stock_list_values(nemonico, startDate, endDate):
    setting = get_config()
    r = requests.get(setting["url_bvl"]["url_historico"].format(nemonico,startDate,endDate))
    lista_values = json.loads(r.text)
    return lista_values


def insertar_movimientos_del_dia():
    setting = get_config()
    #Request Method: POST
    # variables detalle
    payload = {
        "companyCode": "",
        "inputCompany": "",
        "isToday": "true",
        "sector": ""
    }

    r = requests.post(setting["url_bvl"]["url_bvl_movimientos_del_dia"], json=payload)
    vinfo = json.loads(r.text)
    val = ""
    for  v in vinfo:
        v1 = ""
        if "sectorCode" in v:
            v1 = v["sectorCode"]
        v2 = ""
        if "sectorDescription" in v:
            v2 = v["sectorDescription"]
        v3 = ""
        if "buy" in v:
            v3 = v["buy"]
        v4 = ""
        if "sell" in v:
            v4 = v["sell"]
        val += "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(
            v['companyCode'],v['companyName'],v['shortName'],
            v['nemonico'],v1,v2,
            '',v['previousDate'],v3,v4,
            v['previous'],v['negotiatedQuantity'],v['negotiatedAmount'],
            v['negotiatedNationalAmount'],v['operationsNumber'],v['currency'],
            v['unity'],v['segment'],v['createdDate'])

    if len(val)>0:
        insert_row_stockistoday(val[:-1])
