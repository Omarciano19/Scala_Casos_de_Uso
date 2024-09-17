# Resultados del curso "BlueTab University Casos de Uso".

Este es el resultado de la capacitación por BlueTab para la posición de casos de uso, en este se desarrollo un notebook en Scala y posteriormente un script para el ejercicio en particular con enfoque en ETL.

**Los datos de este ejercicio son privados y por lo tanto nunca publicados  ni compartidos con los capacitados.**

Este archivo Scala realiza un procesamiento detallado de movimientos financieros, generando identificadores únicos para cada transacción, categorizando los movimientos (capital, intereses, impuestos), y uniendo los datos con información de cuentas y monedas, con el fin de preparar los datos para un análisis financiero conforme los siguientes [requerimientos](./Data_Model_Caso_de_uso.xlsx) de un peticionario:

![image](https://github.com/user-attachments/assets/bdaf50bd-70e3-4030-b58d-559ffca7e41f) *Requerimientos para el caso de uso.*



## Sinopsis

En este ejercicio final se ponen en practica las habilidades de ETL exploradas en el curso para un procesamiento complejo de datos financieros utilizando Apache Spark:

### Extract (Extracción): 
El código extrae datos de múltiples fuentes de almacenamiento en formato Parquet, como:

- ```/data/master/mdco/data/t_mdco_receipts_dtmvr```

- ```/data/master/mdco/data/t_mdco_ugdtmov```

- ```/data/master/mdco/data/t_mdco_ugdtmae```

- Entre otras.

#### Lectura de datos
Se cargan varios conjuntos de datos en formato Parquet desde rutas específicas del sistema, relacionados con movimientos financieros, cuentas contractuales y otros detalles como seguros y gastos generales.

### Transform (Transformación):
La mayor parte del código está dedicada a transformar los datos para generar una estructura que sea útil para un análisis  posterior, tal como es solicitado por el stakehlder. Algunas transformaciones incluyen:

#### Filtrado y Selección:
Cada conjunto de datos se filtra por una fecha específica (odate), y se seleccionan las columnas necesarias para el procesamiento, como IDs de cuenta, fechas, montos de movimientos, secuencias de movimiento, etc.

#### Generación de claves: 
Utilizando funciones como concat, se generan identificadores únicos (g_movement_id) para las diferentes transacciones financieras, combinando información de múltiples columnas.

#### Cálculos Financieros:
Se realizan diversas agregaciones y filtrados en base a diferentes tipos de movimientos financieros:

- -Capital: Movimientos de capital.
- -Impuestos: Sumas de impuestos sobre intereses, comisiones, etc.
- -Intereses: Movimientos relacionados con intereses.
- -Comisiones: Comisiones pagadas.
- -Gastos: Gastos de correo y otros.
- -Cálculo de montos acumulados.

#### Asignación de Moneda:
Los movimientos se asignan a una moneda específica basada en el ```ID``` de la cuenta, con lógica para diferenciar entre pesos mexicanos (MXN) y otras monedas.

#### Unión de DataFrames: 
Se combinan los diferentes DataFrames procesados para obtener una vista completa de los movimientos, utilizando uniones (join) con claves como el ```ID``` de cuenta o el ```ID``` de movimiento.



### Load (Carga):
El codigo no incluye una carga de forma explicita en un destino particular como una base de datos a petición del stake holder, pues este "caso de uso". En este caso, es probable que el resultado final ```(MRVComplete.union(MOVWithCurrency))``` esté listo para ser guardado o utilizado en análisis adicionales.


#### Unión Final:
Se unen los datos de movimientos generales (MRVComplete) con los datos de movimientos específicos ```(MOVWithCurrency)```, formando un conjunto de datos unificado con detalles de préstamos y movimientos financieros completos.
