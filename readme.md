# SCRAPEO DE BOLSA DE VALORES DE LIMA

Los siguientes dos archivos "get_finanzas.py" y "get_stockhistory.py" obtienen información de la bolsa de valores de lima atravez de los api's de servicio.

### Requisitos previos

- Tener python instalado
- Instalar las librerias del archivo "requirement.txt"
- Configurar las variables de entorno de la base de datos: HOST, DATABASE, USER, PORT, PASSWORD

## Ejecución
- ejecutar el comando: python get_finanzas.py 0
- ejecutar el comando: python get_finanzas.py 1
- ejecutar el comando: python get_finanzas.py 2
- ejecutar el comando: python get_finanzas.py 3
- ejecutar el comando: python get_stockhistory.py

### Contenido get_finanzas.py 0
Obtiene información de todos las acciones de la BVL.

### Contenido get_finanzas.py 1
Agregue las descripción de cada compañoa de la BVL.

### Contenido get_finanzas.py 2
Obtención de los montos de dividendos de las empresas de la BVL.

### Contenido get_finanzas.py 3
Obtención de los radios de las empresas de la BVL

