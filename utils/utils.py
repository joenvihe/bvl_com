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

def select_companyStock_with_code():
    query = """
    SELECT DISTINCT a."rpjCode",a.companyCode,a.companyName,a.nemonico,a.sectorCode,a.sectorDescription
    FROM companyStock a
    WHERE a."rpjCode" IS NOT NULL
    """

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[0])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode


def select_companyStock_opciones(op,codigo = ""):
    if op == 1:
        query = """
        SELECT a.companyCode,a.companyName,a.nemonico,a.sectorCode,a.sectorDescription
        FROM companyStock a
        WHERE a."rpjCode" IS NULL
        """
    elif op == 2:
        query = """
        SELECT a.companyCode,a.companyName,a.nemonico,a.sectorCode,a.sectorDescription
        FROM companyStock a
        WHERE a."rpjCode" IS NOT NULL
        """
    elif op == 3:
        query = """
        select datedelivery from stockcompanyvalue  where codigo = '{}'  order by 1 desc  limit 1
        """.format(codigo)
        #print(query)


    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[0])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode

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
        #print(count, "Record inserted")
        
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
        #print(count, "Record inserted")
        
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
        #print(count, "Record inserted")
        
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


def update_row_companyStock(code,website,desc,compcode):
    try:
        query = """
                UPDATE companyStock
                SET "rpjCode" = '{}',
                    website = '{}',
                    "esActDescription" = '{}'
                WHERE companyCode = '{}' """.format(code,website,desc,compcode)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        #print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     


def actualizar_datos_compania(codigo):
    setting = get_config()
    r = requests.get(setting["url_bvl"]["url_bvl_info_compania"].format(codigo))
    vinfo = json.loads(r.text)
    if "rpjCode" in vinfo:
        update_row_companyStock(vinfo["rpjCode"],vinfo["website"],vinfo["esActDescription"],codigo) 


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

def insert_row_stockvalues(lst_row):
    try:        
        query = """INSERT INTO public.stockcompanyvalue(codigo,nemonico,benefitValue,benefitType,isin,dateEntry,dateAgreement,
                                                        dateCut,dateRegistry,dateDelivery,coin,secMovBe,secMovDi,notesValue,
                                                        notesLaw,notesAgreement,notesCut,notesRegistry,notesDelivery)
	               VALUES {} """.format(lst_row)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        #print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     


def insertar_dividendos_x_compania(codigo):
    setting = get_config()
    r = requests.get(setting["url_bvl"]["url_dividendos_x_compania"].format(codigo)) 
    vinfo = json.loads(r.text)
    #print(vinfo)
    ldate = select_companyStock_opciones(3,codigo)
    val  = ""
    entro = False
    if len(vinfo)>0:
        try:
            if "listBenefit" in vinfo[0]: 
                for v in vinfo[0]["listBenefit"]:
                    try:
                        if len(ldate) > 0 and v["dateDelivery"] > ldate[0] and str(v["dateDelivery"]) != str(ldate[0]):
                            entro = True  
                            val += "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(codigo,v['nemonico'],v['benefitValue'],v['benefitType'],v['isin'],v['dateEntry'],v['dateAgreement'],v['dateCut'],v['dateRegistry'],v['dateDelivery'],v['coin'],v['secMovBe'],v['secMovDi'],v['notesValue'],v['notesLaw'],v['notesAgreement'],v['notesCut'],v['notesRegistry'],v['notesDelivery'])
                        elif len(ldate) == 0:
                            entro = True
                            val += "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(codigo,v['nemonico'],v['benefitValue'],v['benefitType'],v['isin'],v['dateEntry'],v['dateAgreement'],v['dateCut'],v['dateRegistry'],v['dateDelivery'],v['coin'],v['secMovBe'],v['secMovDi'],v['notesValue'],v['notesLaw'],v['notesAgreement'],v['notesCut'],v['notesRegistry'],v['notesDelivery'])
                    except Exception as e:
                        entro = False
        except Exception as e:
            print(e)
            entro = False
        if entro:
            insert_row_stockvalues(val[:-1])


def select_ratios_financieros(codigo = ""):
    query = """
    SELECT year, codigo, dratio, nimportea
	FROM public.ratios_financieros
    WHERE codigo = '{}'
    ORDER BY year DESC
    LIMIT 1
    """.format(codigo)

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append(row[0])
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode


def insert_row_ratios_financieros(lst_row):
    try:
        
        query = """INSERT INTO public.ratios_financieros(codigo,dRatio,year,nImporteA)
	               VALUES {} """.format(lst_row)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        #print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)   

def insertar_declaracion_financiera_x_compania(codigo):
    setting = get_config()
    r = requests.get(setting["url_bvl"]["url_declaracion_financiera_x_compania"].format(codigo)) 
    data = json.loads(r.text)   
    l = select_ratios_financieros(codigo)
    if len(l)>0:
        ratio_year = int(l[0])
    else:
        ratio_year = 1990        
    lst_radios = []
    str_row = ""        
    for v in data:
        for f in v["finantialIndexYears"]:
            if int(f["year"]) > ratio_year: 
                drad = {}
                drad["codigo"] = codigo
                drad["dRatio"] = v["dRatio"]
                drad["year"] = f["year"]
                drad["nImporteA"] = f["nImporteA"]
                lst_radios.append(drad) 
                str_row += "('{}','{}','{}','{}'),".format(codigo,v["dRatio"],f["year"],f["nImporteA"])
    if len(str_row) > 0:
        insert_row_ratios_financieros(str_row[:-1])



def select_doc_financieros(codigo = ""):
    query = """
    SELECT yearperiod, period, documentname, documentorder, documenttype, path, rpjcode, 
           eefftype, caccount, maintitle, numbercolumns, title, value1
	FROM public.doc_financieros
    WHERE rpjcode = '{}'
    ORDER BY yearperiod DESC, period DESC
    LIMIT 1
    """.format(codigo)

    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query) 

    list_stockCode = []
    row = cur.fetchone()
    while row is not None:
        #print(row)
        list_stockCode.append("{}{}".format(row[0],row[1]))
        row = cur.fetchone()

    cur.close()
    conn.close()
    
    return list_stockCode

def insert_row_doc_financieros(lst_row):
    try:
        
        query = """INSERT INTO public.doc_financieros(yearPeriod,period,documentName,documentOrder,documentType,path,rpjCode,eeffType,caccount,mainTitle,numberColumns,title,value1)
	               VALUES {} """.format(lst_row)
        # connect to the PostgreSQL server
        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        
        count = cur.rowcount
        #print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)   

def insertar_resultado_x_quarter_x_compania(codigo,anho,quarter):
    # variables detalle
    payload = {
        "page": "1",
        "period": "1",
        "periodAccount": quarter, #cuatrimestre
        "rpjCode": codigo,
        "search": "",
        "size": "12",
        "type": "1",
        "yearPeriod": anho #año
    } # ejempplo 2021 1  or 2021 2 or 2021 3 or 2021 4, etc

    setting = get_config()
    r = requests.get(setting["url_bvl"]["url_declaracion_financiera_general"], json=payload)
    lista_values = json.loads(r.text)
    l = select_doc_financieros(codigo)
    if len(l)>0:
        doc_year = int(l[0])
    else:
        doc_year = 19901

    lst_val = []
    str_row = ""
    for v in lista_values["content"]:
        if "document" in v:
            #print(v["document"])
            for i in v["document"]:
                valor = int("{}{}".format(v["yearPeriod"],v["period"]))
                if valor >doc_year:
                    dval ={}
                    dval["yearPeriod"] = v["yearPeriod"]
                    dval["period"] = v["period"]  #trimestr 1,2,3,4
                    dval["documentName"] = v["documentName"] 
                    dval["documentOrder"] = v["documentOrder"] 
                    dval["documentType"] = v["documentType"] 
                    dval["path"] = v["path"] 
                    dval["rpjCode"] = v["rpjCode"]
                    dval["eeffType"] = v["eeffType"]   
                    dval["caccount"] = i["caccount"]   
                    dval["mainTitle"] = i["mainTitle"]   
                    dval["numberColumns"] = i["numberColumns"]   
                    dval["title"] = i["title"]   
                    dval["value1"] = i["value1"]   
                    lst_val.append(dval)
                    str_row += "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),".format(v["yearPeriod"], v["period"], v["documentName"], v["documentOrder"], v["documentType"], v["path"], v["rpjCode"], v["eeffType"] , i["caccount"], i["mainTitle"] , i["numberColumns"], i["title"] , i["value1"])

    if len(str_row) > 0:
        insert_row_doc_financieros(str_row[:-1])
