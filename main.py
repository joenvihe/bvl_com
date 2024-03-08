import utils.utils as u 
from datetime import datetime

def obtener_lista_actualizada_de_acciones_en_bolsa():
    # Obtengo la info de la web
    stock_list = u.get_stock_list()
    print(stock_list)
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
    print(nemonico)
    lsh = u.select_stockHistory(nemonico)
    print(lsh)
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
            print(e)
        
    str_values = str_values[:-1]
    if len(str_values)>0:
        u.insert_row_stockHistory(str_values)
    

if __name__ == "__main__":
    #create_tables() -- SOLO EJECUTAR 1 VEZ
    lista_de_acciones_en_bolsa = obtener_lista_actualizada_de_acciones_en_bolsa()
    # Registro la data historica, de todas las acciones
    for nemonico in lista_de_acciones_en_bolsa:
        lista_precios_x_accion_x_fecha = obtener_lista_de_precios_x_accion_x_rango_fechas(nemonico)
        insertar_lista_precios_x_accion(lista_precios_x_accion_x_fecha)

