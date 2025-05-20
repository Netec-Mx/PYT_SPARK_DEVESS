# Práctica 4. Acciones sobre RDD

##**Objetivo de la práctica:**

Al finalizar la práctica serás capaz de:
- Entender la función de las acciones y aplicarlas sobre RDD.

##**Tiempo aproximado:**
- 45 minutos.

##**Prerrequisitos:**

- Acceso al ambiente Linux (credenciales provistas en el curso) o Linux local con interfaz gráfica.
- Conexión a internet.

##**Contexto:**

Las acciones en PySpark son operaciones que disparan la ejecución de las transformaciones acumuladas en un RDD.

Algunas acciones comunes incluyen collect(), count(), first(), take(), reduce(), foreach(), y saveAsTextFile().

Las acciones devuelven resultados al programa driver o escriben datos en un sistema externo. A diferencia de las transformaciones (como map, filter, etc.), que son perezosas (lazy) y no se ejecutan hasta que se llama a una acción, las acciones disparan la ejecución de todas las transformaciones acumuladas en el RDD.

**Instrucciones:**

## Tarea 1: Aplicar acciones en un archivo

En este ejemplo, se leerá el contenido de un archivo para contar las palabras

Iniciar PyCharm.

`pycharm-community`

Introducir el siguiente código ajustando la ruta del archivo a la ruta local.

```
from pyspark import SparkContext

# Inicializar SparkContext
sc = SparkContext("local", "Ejemplo RDD desde archivo")

# Cargar un RDD desde un archivo de texto
rdd = sc.textFile("/home/miguel/data/TotalSalesRed/Sales2018.csv")

# Transformación: Dividir cada línea en palabras
rdd_palabras = rdd.flatMap(lambda linea: linea.split(","))

# Transformación: Convertir palabras a minúsculas y mayúsculas
rdd_minusculas = rdd_palabras.map(lambda palabra: palabra.lower())
rdd_mayusculas = rdd_palabras.map(lambda palabra: palabra.upper())

# Acción: Contar la cantidad de palabras
cantidad_palabras = rdd_minusculas.count()

# Acción: Recopilar las primeras 10 palabras
primerasMin_palabras = rdd_minusculas.take(10)
primerasMay_palabras = rdd_mayusculas.take(10)

# Mostrar resultados
print("Cantidad de palabras:", cantidad_palabras)
print("Primeras 10 palabras minúsculas:", primerasMin_palabras)
print("Primeras 10 palabras minúsculas:", primerasMay_palabras)

# Cerrar SparkContext
sc.stop()
```

<img src="./media/image1.png" style="width:4.61441in;height:4.36693in" />

<img src="./media/image2.png" style="width:6.1375in;height:0.58819in" />

**Validación de archivos por columnas**

Un uso que se pueden aplicar en archivos CSV, es el contar filas que tienen la cantidad válida de columnas. Para esto, copiaremos y modificaremos uno de los archivos para reducir la cantidad de columnas.

Abrir una ventana de terminal nueva. En el directorio de data, pasamos al directorio TotalSalesRed

`cd data/TotalSalesRed`

Copiar el archivo **Sales2018** a **Sales2018Redf.csv**.

cp Sales2018.csv Sales2018f.csv

<img src="./media/image3.png" style="width:6.1375in;height:0.99722in" />

Con cualquier editor, abir el archivo y remover los campos en diferentes renglones para tener filas de menos de 5 campos. Recordar que los campos están separados por comas (,).

<img src="./media/image4.png" style="width:3.46484in;height:2.26822in" />

Salvar el archivo y regresar a PyCharm (Ctrl+O para salvar y CTRL+X para salir).

Probar el siguiente código:

```
from pyspark import SparkContext

#Inicializar SparkContext
sc = SparkContext("local", "Ejemplo RDD desde CSV")

# Cargar un RDD desde un archivo de texto
rdd = sc.textFile("/home/miguel/data/TotalSalesRed/Sales2018f.csv")

# Transformación: Dividir cada línea en columnas
rdd_columnas = rdd.map(lambda linea: linea.split(","))

# Transformación: Filtrar filas que tienen más de 2 columnas
rdd_filtrado = rdd_columnas.filter(lambda columnas: len(columnas) &gt; 6)

# Acción: Contar el número de filas válidas
cantidad_filas = rdd_filtrado.count()

# Acción: Recopilar las primeras 3 filas
primeras_filas = rdd_filtrado.take(3)

# Mostrar resultados
print("Cantidad de filas válidas:", cantidad_filas)
print("Primeras 3 filas:", primeras_filas)

# Cerrar SparkContext
sc.stop()
```

<img src="./media/image5.png" style="width:4.38183in;height:3.22266in" />

<img src="./media/image6.png" style="width:5.61721in;height:0.39769in" />

**Extraer y calcular valores desde un csv**

Se pueden realizar diferentes operaciones con los valores leídos del rdd

```
from pyspark import SparkContext
sc = SparkContext("local", "Carga desde CSV")

# Cargar un archivo CSV como RDD
rdd = sc.textFile("/home/miguel/data/TotalSalesRed/Sales2020.csv")

#Dividir cada línea en columnas
rdd_columnas = rdd.map(lambda linea: linea.split(","))

#Filtrar la cabecera (si existe)
cabecera = rdd_columnas.first() # Obtener la primera línea (cabecera)
rdd_datos = rdd_columnas.filter(lambda linea: linea != cabecera)

# Mapear a la columna numérica (por ejemplo, la columna 2)
rdd_numeros = rdd_datos.map(lambda linea: float(linea[10]))

#Control simple para mostrar el contenido del rdd
for row in rdd_numeros.collect():
    print(row)

#Contar el número de elementos
total_elementos = rdd_numeros.count()

#Sumar todos los valores
suma_total = rdd_numeros.reduce(lambda x, y: x + y)

# Calcular el promedio
promedio = suma_total / total_elementos
print(f"El promedio es: {promedio}")
print(f"La suma es: {suma_total}")

# Cerrar SparkContext
sc.stop()
```

<img src="./media/image7.png" style="width:5.80248in;height:6.19509in" />

<img src="./media/image8.png" style="width:4.28194in;height:1.72917in" />

**Obtener el pedido, fecha cliente, país, cantidad de producto, precio. Calcular importe y total de ventas**

```
from pyspark import SparkContext

sc = SparkContext("local", "CargaCSV")

rdd = sc.textFile("/home/miguel/data/Sales.csv") # Cargar el archivo CSV en un RDD

header = rdd.first() # Obtener la primera línea (encabezado)
rdd_data = rdd.filter(lambda line: line != header) # Filtrar el encabezado

rdd_datos = rdd_data.map(lambda line: line.split(","))# Transformación: Parsear el CSV (dividir cada línea por comas)

print("Datos interpretados:")
print(rdd_datos.take(5))

# Transformación: Calcular el total de ventas (precio * cantidad) por producto
rdd_ventas = rdd_datos.map(lambda x: (x[0],x[1],x[2],x[4],x[7],x[10], x[11], float(x[10]) * float(x[11])))

# Mostrar el total de ventas por producto
print("Ventas por producto:") 
for row in rdd_ventas.collect():
    print(row)

# Acción: Contar el número total de ventas
print("Número total de ventas:") 
print(rdd_ventas.count())

rdd_fechas = rdd_ventas.map(lambda cols: (cols[1], 1)) # Transformación: Mapear a pares (fecha, 1)

rdd_ventas_por_dia = rdd_fechas.reduceByKey(lambda x, y: x + y) # Transformación: Contar número ventas por fecha

for fecha, ventas in rdd_ventas_por_dia.collect(): # Acción: Recopilar y mostrar las ventas por día
print(f"Fecha: {fecha}, Ventas: {ventas}")

**sc.stop()**
```

<img src="./media/image9.png" style="width:5.56909in;height:3.26974in" />

<img src="./media/image10.png" style="width:5.3609in;height:1.80941in" />

## ***Fin del laboratorio***
