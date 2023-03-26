import requests
import json

from requests.api import request
import psycopg2
from dotenv import load_dotenv   #for python-dotenv method
load_dotenv()                    #for python-dotenv method
import os 
from datetime import datetime
import pandas as pd
import sys

URL_INFO = "https://dataondemand.bvl.com.pe/v1/issuers/{}/info"
URL_VALUE = "https://dataondemand.bvl.com.pe/v1/issuers/{}/value"
URL_COD = "https://dataondemand.bvl.com.pe/v1/financial-statements/{}"
URL = "https://dataondemand.bvl.com.pe/v1/financial-statements/"

#COD = "OE2305"
#COD = "B60051"
COD = "B20003"
YEAR_PERIOD = "2000"


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
    conn = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password)
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


def select_companyStock(op,codigo = ""):
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
        print(query)


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
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     


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
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)     


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
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)   


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
        print(count, "Record inserted")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(e)   

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

if __name__ == "__main__":
    #python get_finanzas.py 0 :stockistoday
    #python get_finanzas.py 1 :companystock
    #python get_finanzas.py 2 :companystock

    par = sys.argv # 1 el año
    # parametro 1 y 2:
    # 1 actualizar el update del codigo
    # 2 2000 , dos variables 

    if par[1] == "0": # todo de hoy
        URL_HOME = "https://dataondemand.bvl.com.pe/v1/stock-quote/home"
        #Request Method: POST
        # variables detalle
        payload = {
            "companyCode": "",
            "inputCompany": "",
            "isToday": "true",
            "sector": ""
        }

        r = requests.post(URL_HOME, json=payload)
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
                v['lastDate'],v['previousDate'],v3,v4,
                v['previous'],v['negotiatedQuantity'],v['negotiatedAmount'],
                v['negotiatedNationalAmount'],v['operationsNumber'],v['currency'],
                v['unity'],v['segment'],v['createdDate'])

        if len(val)>0:
            insert_row_stockistoday(val[:-1])

    if par[1] == "1":
        lst_comp = select_companyStock(1)
        for codigo in lst_comp:
            if codigo != "XXX":
                r = requests.get(URL_INFO.format(codigo))
                vinfo = json.loads(r.text)
                update_row_companyStock(vinfo["rpjCode"],vinfo["website"],vinfo["esActDescription"],codigo) 

    if par[1] == "2":
        lst_comp = select_companyStock(2)
        for codigo in lst_comp:
            r = requests.get(URL_VALUE.format(codigo))
            vinfo = json.loads(r.text)
            #print(vinfo)
            ldate = select_companyStock(3,codigo)
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
            #break
            
    if par[1] == "3":
        lst_code = select_companyStock_with_code()
        arrV = []
        for codigo in lst_code:
            r = requests.get(URL_COD.format(codigo))
            print(URL_COD.format(codigo))
            data = json.loads(r.text)

            
            l = select_ratios_financieros(codigo)
            if len(l)>0:
                ratio_year = int(l[0])
            else:
                ratio_year = 1990
            #print(ratio_year)
            
            lst_radios = []
            # variables generales
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

            #print(str_row)
            if len(str_row) > 0:
                insert_row_ratios_financieros(str_row[:-1])

            #print(lst_radios)
            #break


            #df = pd.DataFrame.from_dict(data, orient='columns')
            #print(df)

    if par[1] == "4":
        lst_code = select_companyStock_with_code()
        arrV = []
        for codigo in lst_code:
    
            # variables detalle
            payload = {
                "page": "1",
                "period": "1",
                "periodAccount": par[3], #cuatrimestre
                "rpjCode": codigo,
                "search": "",
                "size": "12",
                "type": "1",
                "yearPeriod": par[2] #año
            } # ejempplo 2021 1  or 2021 2 or 2021 3 or 2021 4, etc

            r = requests.post(URL, json=payload)
            lista_values = json.loads(r.text)
            #print(lista_values)

            #break
            #df = pd.DataFrame.from_dict(lista_values, orient='columns')
            #print(r.status_code)
            #print(df)
            
            
            l = select_doc_financieros(codigo)
            if len(l)>0:
                doc_year = int(l[0])
            else:
                doc_year = 19901
            #print(doc_year)
            

            #print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            lst_val = []
            str_row = ""
            for v in lista_values["content"]:
                #print(v["document"])
                if "document" in v:
                    #print(v["document"])
                    for i in v["document"]:
                        valor = int("{}{}".format(v["yearPeriod"],v["period"]))
                        #print(valor) 
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

            #print(str_row)
            if len(str_row) > 0:
                insert_row_doc_financieros(str_row[:-1])


            #df = pd.DataFrame(lst_val)
            #df.to_csv("eeff.csv")
            #break
