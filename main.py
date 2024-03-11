import utils.utils as u 
from datetime import datetime

def obtener_lista_actualizada_de_acciones_en_bolsa():
    # Obtengo la info de la web
    stock_list = u.get_stock_list()
    # Obtengo la lista de codigos grabado en la BD
    listStockCode = u.select_companyStock()
    # Inserto todos los row que faltaban.
    entro = False
    for i in stock_list:
        if i["nemonico"] not in listStockCode:
            #print(i)
            u.insert_row_companyStock(i)
            entro = True
    if entro:
        #obtengo la lista actualizada
        listStockCode = u.select_companyStock()

    return listStockCode

def obtener_lista_de_precios_x_accion_x_rango_fechas(nemonico):
    lsh = u.select_stockHistory(nemonico)
    if len(lsh) > 0:
        startDate = lsh[0]
    else:
        startDate = "2000-01-01"
    endDate = datetime.today().strftime('%Y-%m-%d')
    list_values = u.get_stock_list_values(nemonico, startDate, endDate)
    return list_values 

def insertar_lista_precios_x_accion(list_values):
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
            print("14")
            print(e)
        
    str_values = str_values[:-1]
    if len(str_values)>0:
        u.insert_row_stockHistory(str_values)

def actualizar_datos_compania_masivamente():
    lst_comp = u.select_companyStock_opciones(1)
    for codigo in lst_comp:
        if codigo != "XXX":
            u.actualizar_datos_compania(codigo)

def insertar_datos_dividendos_x_compania_masivamente():
    lst_comp = u.select_companyStock_opciones(2)
    for codigo in lst_comp:
        u.insertar_dividendos_x_compania(codigo)

def insertar_declaracion_financiera_x_compania_masivamente():
    lst_code = u.select_companyStock_with_code()
    for codigo in lst_code:
        u.insertar_declaracion_financiera_x_compania(codigo)

def lista_year_quarter(year_quarter):
    now = datetime.now()
    anho_actual = now.year
    lista_anho_quarter = []
    for year in range(year_quarter,anho_actual+1):
        for quarter in range(1,5):
            valor = "{}{}".format(year,quarter)
            lista_anho_quarter.append(int(valor))
    return lista_anho_quarter

def insertar_resultado_x_quarter_x_compania_masivamente():
    lst_code = u.select_companyStock_with_code()
    print("seleciona codigo")
    for codigo in lst_code:
        l = u.select_doc_financieros(codigo)
        if len(l)>0:
            year_quarter = int(l[0])
        else:
            year_quarter = 19901
        lista_de_anhos_y_quarter = lista_year_quarter(int(str(year_quarter)[0:4]))
        for i in lista_de_anhos_y_quarter:
            if i > year_quarter:
                anho = str(i)[0:4]
                quarter = str(i)[-1]
                u.insertar_resultado_x_quarter_x_compania(codigo,anho,quarter)

if __name__ == "__main__":
    #create_tables() -- SOLO EJECUTAR 1 VEZ
    print("inicia insertar movimientos del dia")
    try:
        u.insertar_movimientos_del_dia()
    except Exception as e:
        print("9")
        print(e)
    
    try:
        print("inicia insertar precio de historico por accion")
        lista_de_acciones_en_bolsa = obtener_lista_actualizada_de_acciones_en_bolsa()
        # Registro la data historica, de todas las acciones
        for nemonico in lista_de_acciones_en_bolsa:
            lista_precios_x_accion_x_fecha = obtener_lista_de_precios_x_accion_x_rango_fechas(nemonico)
            insertar_lista_precios_x_accion(lista_precios_x_accion_x_fecha)
    except Exception as e:
        print("10")
        print(e)

    print("inicia actualizar datos de compañia masivamente")
    try:
        actualizar_datos_compania_masivamente()
    except Exception as e:
        print("11")
        print(e)

    print("inicia insertar datos de dividendos por compañia")
    try:
        insertar_datos_dividendos_x_compania_masivamente()
    except Exception as e:
        print("12")
        print(e)

    #try:
    #    insertar_declaracion_financiera_x_compania_masivamente()
    #except Exception as e:
    #    print(e)
    
    print("insertar datos de resultados por cuarto")
    try:
        insertar_resultado_x_quarter_x_compania_masivamente()
    except Exception as e:
        print("13")
        print(e)




