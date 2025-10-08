# Práctica 7. Aspectos avanzados

## Objetivos
Al finalizar la práctica, serás capaz de:
- Aplicar técnicas avanzadas a conjuntos de datos, como `shuffling`, `accumulators`, `partitioning` y `broadcast` de variables.


## Duración aproximada
- 60 minutos.

## Prerrequisitos

-   Acceso a ambiente Linux (credenciales provistas en el curso) o Linux
    local con interfaz gráfica.

-   Tener los archivos de datos.

-   Completar el laboratorio 1.

## Contexto

-   La optimización de operaciones con RDD en PySpark es crucial para
    mejorar el rendimiento de las aplicaciones, especialmente cuando se
    trabajan con grandes volúmenes de datos.

-   El particionamiento es la forma en que los datos se dividen y
    distribuyen en el clúster. Un buen particionamiento puede mejorar el
    rendimiento al permitir un paralelismo eficiente y minimizar el
    shuffling.

## Instrucciones

### Tarea 1. Crear y ajustar el número de particiones

- Crea un RDD con un número específico de particiones.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Particionamiento")

\# Crear un RDD con 4 particiones

rdd = sc.parallelize(range(10), 4)

\# Ver el número de particiones

print("Número de particiones:", rdd.getNumPartitions())

\# Mostrar el contenido de cada partición

print(rdd.glom().collect())

\# Cerrar SparkContext

sc.stop()
```

![](./media/image1.png)

![](./media/image2.png)

En este ejemplo:

-   `sc.parallelize(range(10), 4)` crea un RDD con 10 elementos
    divididos en 4 particiones.

-   `glom()` muestra el contenido de cada partición.

**Cargar un archivo csv con 4 particiones**

```
from pyspark import SparkContext

sc = SparkContext("local", "Cargar CSV con 4 particiones")

ruta_csv = "/home/miguel/data/Model/Products.csv"

\# Cargar el archivo CSV en un RDD con 4 particiones

rdd = sc.textFile(ruta_csv, minPartitions=4)

\# Verificar el número de particiones

print("Número de particiones:", rdd.getNumPartitions()) \# Debería ser 4

\# Transformación: Dividir cada línea en columnas

rdd_columnas = rdd.map(lambda linea: linea.split(","))

\# Filtrar la cabecera (si existe)

cabecera = rdd_columnas.first() \# Obtener la primera línea (cabecera)

rdd_datos = rdd_columnas.filter(lambda linea: linea != cabecera) \#
Filtrar la cabecera

\# Ver el contenido de cada partición

print("Contenido de las particiones:")

particiones = rdd_datos.glom().collect()

for i, particion in enumerate(particiones):

print(f"Partición {i}: {particion}")

\# Cerrar SparkContext

sc.stop()
```

![](./media/image3.png)

![](./media/image4.png)

En este ejemplo:

-   `sc.textFile(ruta_csv, minPartitions=4)` para cargar el archivo
    CSV en un RDD con al menos 4 particiones.

-   `getNumPartitions()` para verificar RDD el número de particiones.

-   `glom()` para mostrar el contenido de cada partición.

**Nota. El número real de particiones puede ser mayor que 4 si el
archivo es grande, ya que `minPartitions` es un mínimo, no un máximo.**

## Tarea 2. Reparticionamiento

El reparticionamiento permite ajustar el número de particiones de un
RDD. Esto es útil para optimizar el paralelismo o reducir la sobrecarga.

Para reparticionar, se pueden usar `repartition()` o `coalesce()`.

-   `repartition()` aumenta o reduce el número de particiones, pero
    siempre causa shuffling.

-   `coalesce()` reduce el número de particiones sin shuffling (más
    eficiente que `repartition`).

```
from pyspark import SparkContext

sc = SparkContext("local\[4\]", "Particionamiento")

rdd = sc.textFile("/home/miguel/data/Model/Customers.csv") \# Cargar el
archivo CSV en un RDD

\# Mostrar el número de particiones

print(rdd.getNumPartitions())

\# Reparticionar el RDD

rdd_reparticionado1 = rdd.repartition(10) \# Esto provocaría shuffling

rdd_reparticionado2 = rdd.coalesce(10) \# No provoca shuffling

print(rdd_reparticionado1.getNumPartitions())

\# Mostrar el contenido de cada partición

print(rdd_reparticionado1.glom().collect())

print(rdd_reparticionado2.getNumPartitions())

print(rdd_reparticionado1.glom().collect())

\# Cerrar SparkContext

sc.stop()
```

![](./media/image5.png)

![](./media/image6.png)

En este ejemplo:

-   `repartition(10)` ajusta el número de particiones a 10, pero causa
    shuffling.

-   `coalesce(10)` ajusta el número de particiones a 10 sin shuffling.

## Tarea 3. Optimización con particionamiento y persistencia

El control de operaciones implica gestionar cómo se ejecutan las
transformaciones y acciones en el clúster.

-   **Persistencia de RDD:** usa `persist()` o `cache()` para evitar
    recalcular RDD que se usan varias veces.

-   **Control del orden de ejecución:** las transformaciones son
    perezosas `(lazy)`, pero puedes forzar la ejecución con acciones como
    `count()` o `collect()`.

```
from pyspark import SparkContext, StorageLevel

\# Inicializar SparkContext

sc = SparkContext("local", "Optimización con Particionamiento")

\# Cargar datos de ventas

rdd = sc.textFile("/home/miguel/data/Sales.csv")

\# Eliminar cabecera

cabecera = rdd.first()

rdd_datos = rdd.filter(lambda linea: linea != cabecera)

\# Transformación: Mapear a (producto, cantidad)

rdd_productos = rdd_datos.map(lambda linea: (linea.split(",")\[6\],
float(linea.split(",")\[10\])))

\# Reparticionar por clave (producto)

rdd_reparticionado = rdd_productos.partitionBy(4)

\# Persistir el RDD para reutilización

rdd_reparticionado.persist(StorageLevel.MEMORY_AND_DISK)

\# Reducción: Calcular total por producto

rdd_total = rdd_reparticionado.reduceByKey(lambda x, y: x + y)

\# Acción: Recopilar resultados

resultados = rdd_total.collect()

\# Mostrar resultados

for producto, total in resultados:

print(f"{producto}: {total}")

\# Liberar persistencia

rdd_reparticionado.unpersist()

\# Cerrar SparkContext

sc.stop()
```


![](./media/image7.png)

![](./media/image8.png)

En este ejemplo, optimizaciones aplicadas:

-   **Particionamiento por clave:** se usa `partitionBy` para distribuir
    los datos por producto.

-   **Persistencia:** el RDD se persiste en memoria y disco para evitar
    recalcularlo.

-   **Reducción local:** `reduceByKey` realiza una reducción local antes
    del shuffling.

### Tarea 4. Reconocer el impacto del shuffling

El `shuffling` es un concepto muy importante en PySpark y Spark debido
a su impacto significativo en el rendimiento de las aplicaciones
distribuidas. Ocurre cuando los datos necesitan ser
redistribuidos entre los nodos del clúster, lo que puede ser una
operación costosa en términos de tiempo y recursos.

El shuffling es el proceso de redistribuir los datos entre los nodos del
clúster para agruparlos o reorganizarlos según una clave. Esto ocurre en
operaciones como:

-   `groupByKey` agrupa los valores por clave.

-   `reduceByKey` reduce los valores por clave.

-   `join` combina dos RDD basados en una clave.

-   `distinct` elimina duplicados.

-   `repartition` cambia el número de particiones.

Durante el `shuffling`, los datos se escriben en disco y se transfieren a
través de la red, lo que lo convierte en una operación costosa.

**Se tiene un RDD con pares clave-valor y se quiere agrupar los valores
por clave.**

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Ejemplo de Shuffling")

\# Crear un RDD con pares clave-valor

rdd = sc.parallelize(\[("a", 1), ("b", 2), ("a", 3), ("b", 4)\])

\# Operación que causa shuffling: groupByKey

rdd_agrupado = rdd.groupByKey()

\# Mostrar los resultados

resultados = rdd_agrupado.collect()

for clave, valores in resultados:

print(f"{clave}: {list(valores)}")

\# Cerrar SparkContext

sc.stop()
```

![](./media/image9.png)

![](./media/image10.png)

En este ejemplo:

-   `groupByKey` causa shuffling porque necesita agrupar todos los
    valores con la misma clave en la misma partición.

-   Durante el `shuffling`, los datos se transfieren entre los nodos del
    clúster.

### Tarea 5. Minimizar shuffling

Minimizar el shuffling es clave para optimizar el rendimiento en
PySpark.

-   **Usar `reduceByKey` en lugar de `groupByKey`:** `reduceByKey` realiza una
    reducción local antes de hacer el shuffling, lo que reduce la
    cantidad de datos transferidos.

-   **`groupByKey`** transfiere todos los datos sin reducción previa.

-   **Evitar operaciones amplias (wide):** operaciones como `cartesian()`
    o `distinct()` pueden causar mucho shuffling. Úsalas solo cuando sea
    estrictamente necesario.

-   **Usar particionamiento adecuado:** un buen particionamiento puede
    reducir el `shuffling` al mantener los datos relacionados en la misma
    partición.

-   **Minimizar el tamaño de los datos:** reduce el tamaño de los datos
    antes de operaciones que causen shuffling. Por ejemplo, filtra o
    proyecta columnas innecesarias.

-   **Usar `coalesce` en lugar de `repartition`:** `coalesce` reduce el
    número de particiones sin shuffling, mientras que `repartition`
    siempre causa `shuffling`.

**Ejemplo**

Se tiene un RDD con registros de ventas y queremos calcular el total de
ventas por producto, minimizando el shuffling.

```
from pyspark import SparkContext

sc = SparkContext("local", "CargaCSV")

rdd = sc.textFile("/home/miguel/data/Sales.csv") \# Cargar el archivo
CSV en un RDD

header = rdd.first() \# Obtener la primera línea (encabezado)

rdd_data = rdd.filter(lambda line: line != header) \# Filtrar el
encabezado

rdd_datos = rdd_data.map(lambda line: line.split(","))# Transformación:
Parsear el CSV (dividir cada línea por comas)

\# Transformación: Obtener pais e importe

rdd_ventas = rdd_datos.map(lambda x: (x\[4\], float(x\[10\]) \*
float(x\[11\])))

for producto in rdd_ventas.collect():

print(producto)

\# Reducción: Calcular total por producto (con reducción local)

rdd_total = rdd_ventas.reduceByKey(lambda x, y: x + y)

\# Acción: Recopilar resultados

resultados = rdd_total.collect()

\# Mostrar resultados

print(" \*\*\* Total por país")

for producto, total in resultados:

print(f"{producto}: {total}")

sc.stop()
```

![](./media/image11.png)

![](./media/image12.png)


### Tarea 6. Broadcast de variables

En PySpark, el broadcast `variables` (variables transmitidas) permite
distribuir grandes estructuras de datos de solo lectura a todos los
nodos del clúster de manera eficiente. Esto evita el costo de enviar
repetidamente la misma información a cada tarea, lo que mejora el
rendimiento.

Cuando trabajamos con RDD en un clúster de Spark, cualquier variable
externa utilizada dentro de una función lambda en una transformación
(`map`, `filter`, etcétera) se enviará a cada tarea ejecutada en los
trabajadores. Si la variable es grande, esto puede generar un uso
excesivo de la red y reducir el rendimiento.

Con broadcast, envías la variable una sola vez y luego todas las
tareas acceden a ella desde su caché local, evitando el reenvío
repetido.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Broadcast Example")

\# Diccionario de referencia (código de producto -\> nombre)

diccionario_productos = {

101: "ProductoA",

102: "ProductoB",

103: "ProductoC"

}

\# Crear la variable broadcast

broadcast_diccionario = sc.broadcast(diccionario_productos)

\# RDD de ventas (código de producto, cantidad)

rdd_ventas = sc.parallelize(\[(101, 2), (102, 3), (103, 1), (101, 5)\])

\# Transformación: Enriquecer el RDD con los nombres de los productos

rdd_enriquecido = rdd_ventas.map(lambda venta: (venta\[0\], venta\[1\],
broadcast_diccionario.value\[venta\[0\]\]))

\# Acción: Recopilar y mostrar los resultados resultados =
rdd_enriquecido.collect() for resultado in resultados:

print(resultado) \# Salida: (101, 2, 'ProductoA'), (102, 3,
'ProductoB'), etc.

\# Liberar la variable broadcast

broadcast_diccionario.unpersist()

\# Cerrar SparkContext

sc.stop()
```

![](./media/image13.png)

![](./media/image14.png)

En este ejemplo:

-   `sc.broadcast(diccionario_productos)` crea un objeto broadcast
    con el diccionario.

-   `broadcast_diccionario.value` accede al diccionario en los
    `executors`.

-   `map()` usa el diccionario broadcast para enriquecer el RDD de
    ventas.

-   `unpersist()` libera los recursos utilizados por el broadcast.

**Broadcast con una lista**

Asume que se tiene una lista de códigos de productos en oferta y se
quiere filtrar un RDD de ventas para incluir solo esos productos.


```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Broadcast Example")

\# Lista de productos en oferta

productos_oferta = \[101, 103\]

\# Crear la variable broadcast

broadcast_oferta = sc.broadcast(productos_oferta)

\# RDD de ventas (código de producto, cantidad)

rdd_ventas = sc.parallelize(\[(101, 2), (102, 3), (103, 1), (101, 5)\])

\# Transformación: Filtrar ventas de productos en oferta

rdd_filtrado = rdd_ventas.filter(lambda venta: venta\[0\] in
broadcast_oferta.value)

\# Acción: Recopilar y mostrar los resultados resultados =
rdd_filtrado.collect() for resultado in resultados:

print(resultado) \# Salida: (101, 2), (103, 1), (101, 5)

\# Liberar la variable broadcast

broadcast_oferta.unpersist()

\# Cerrar SparkContext

sc.stop()
```

![](./media/image15.png)

![](./media/image16.png)

En este ejemplo:

-   `sc.broadcast(productos_oferta)` crea un objeto broadcast con la
    lista de productos en oferta.

-   `broadcast_oferta.value` accede a la lista en los executors.

-   `filter()` usa la lista broadcast para filtrar el RDD de ventas.

-   `unpersist()` libera los recursos utilizados por el broadcast.

### Tarea 7. Broadcast con tabla de parámetros

Ahora, tienes una tabla de parámetros que se requiere usar en múltiples
operaciones.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Broadcast Example")

\# Configuración (diccionario de parámetros)

configuracion = {

"tasa_impuesto": 0.15,

"descuento": 0.10

}

\# Crear la variable broadcast

broadcast_config = sc.broadcast(configuracion)

\# RDD de ventas (producto, monto)

rdd_ventas = sc.parallelize(\[("ProductoA", 100), ("ProductoB", 200),
("ProductoC", 150)\])

\# Transformación: Calcular el monto total con impuestos y descuentos

rdd_calculado = rdd_ventas.map(lambda venta: (

venta\[0\],

venta\[1\],

venta\[1\] \* (1 + broadcast_config.value\["tasa_impuesto"\]), \# Monto
con impuesto

venta\[1\] \* (1 - broadcast_config.value\["descuento"\]) \# Monto con
descuento

))

\# Acción: Recopilar y mostrar los resultados

resultados = rdd_calculado.collect()

for resultado in resultados:

print(resultado) \# Salida: ('ProductoA', 100, 115.0, 90.0), etc.

\# Liberar la variable broadcast

broadcast_config.unpersist()

\# Cerrar SparkContext

sc.stop()
```

En este ejemplo:

-   *`sc.broadcast(configuracion)` crea un objeto broadcast con la
    configuración.

-   `broadcast_config.value` accede a la configuración en los
    executors.

-   `map()` usa la configuración broadcast para calcular montos con
    impuestos y descuentos.

-   `unpersist()` libera los recursos utilizados por el broadcast.

![](./media/image17.png)

![](./media/image18.png)

### Tarea 8. Usar acumuladores

Los acumuladores en PySpark son variables compartidas que permiten sumar
valores de manera eficiente en un clúster distribuido. Son útiles cuando
necesitamos contar eventos, sumar valores o rastrear estadísticas sin
comunicación constante entre los nodos.

**Contar elementos que cumplen una condición**

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Ejemplo Acumulador")

rdd = sc.parallelize(\[1, 2, 3, 6, 7, 8, 9, 10\])

\# Crear un acumulador para contar números mayores que 5

contador = sc.accumulator(0)

\# Transformación: Incrementar el acumulador si el número es mayor que 5

rdd.foreach(lambda x: contador.add(1) if x \> 5 else None)

\# Acción: Mostrar el valor del acumulador

print("Números mayores que 5:", contador.value) \# Salida: 5

\# Cerrar SparkContext

sc.stop()
```


![](./media/image19.png)

![](./media/image20.png)

En este ejemplo:

-   Se crea un acumulador contador inicializado en 0.

-   En la transformación foreach, se incrementa el acumulador si el
    número es mayor que 5.

-   El valor final del acumulador se imprime en el driver.

**Sumar valores de un RDD**

```
from pyspark import SparkContext

sc = SparkContext("local", "Ejemplo Acumulador Suma")

rdd = sc.parallelize(\[1, 2, 3, 4, 5\])

\# Crear un acumulador para la suma

suma_acumulador = sc.accumulator(0)

\# Transformación: Sumar todos los valores al acumulador

rdd.foreach(lambda x: suma_acumulador.add(x))

\# Acción: Mostrar el valor del acumulador

print("Suma total:", suma_acumulador.value) \# Salida: 15

sc.stop()
```

![](./media/image21.png)

![](./media/image22.png)

En este ejemplo:

-   Se crea un acumulador `suma_acumulador` inicializado en 0.

-   En la transformación foreach, se suma cada valor del RDD al
    acumulador.

-   El valor final del acumulador se imprime en el driver.

**Contabilizar inconsistencias de un RDD**

```
from pyspark.sql import SparkSession

spark = SparkSession\\

.builder\\

.appName("Acumuladores")\\

.getOrCreate()

sc = spark.sparkContext

rdd_usuarios = sc.parallelize(\[

(1, "Ana", 25),

(2, "Carlos", -1), \# Edad inválida

(3, "Luis", 30),

(4, None, 40), \# Nombre nulo

(5, "Sofía", 0), \# Edad inválida

\])

\# Crear un acumulador

acumulador_errores = sc.accumulator(0)

\# Filtrar los registros inválidos mientras se incrementa el acumulador

rdd_usuarios_validos = rdd_usuarios.filter(lambda x: not
(acumulador_errores.add(1) if x\[1\] is None or x\[2\] \<= 0 else
False))

\# Ejecutar la transformación y mostrar resultados

print("Usuarios válidos:", rdd_usuarios_validos.collect())

print("Total de registros inválidos:", acumulador_errores.value)
```

![](./media/image23.png)

![](./media/image24.png)

En este ejemplo:

-   Se usa `acumulador_errores.add(1)` dentro de `filter()`.

-   Cuando un usuario tiene datos inválidos (`None` o `edad ≤ 0`), el
    acumulador se incrementa antes de excluir el registro.


