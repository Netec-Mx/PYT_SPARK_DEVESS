# Práctica 5. Uso de funciones de transformación 

## Objetivos
Al finalizar la práctica, serás capaz de:
-Entender y aplicar funciones de transformación sobre RDD.

## Duración aproximada
- 60 minutos

## Prerrequisitos

-   Acceso a ambiente Linux (credenciales provistas en el curso) o Linux
    local con interfaz gráfica.

-   Haber completado el laboratorio 1.

-   Haber descargado los archivos de datos.

## Contexto

Los RDD admiten dos tipos de operaciones: transformaciones, que crean un nuevo conjunto de datos a partir de uno existente y acciones, que
devuelven un valor al programa controlador después de ejecutar un
cálculo en el conjunto de datos.

Todas las transformaciones de Spark son diferidas (lazy), en el sentido
de que no calculan sus resultados de inmediato. En su lugar, solo
recuerdan las transformaciones aplicadas a algún conjunto de datos base
(por ejemplo, un archivo). Las transformaciones solo se calculan cuando
una acción requiere que se devuelva un resultado al programa
controlador. Este diseño permite que Spark se ejecute de forma más
eficiente. Por ejemplo, puedes darte cuenta de que un conjunto de
datos creado a través de un mapa se utilizará en una reducción y
devolverá solo el resultado de la reducción al controlador, en lugar del conjunto de datos mapeado más grande.

### Tipos de transformaciones en RDD

Hay varias clasificaciones de transformaciones asociadas del proceso
que realizan las transformaciones: de ajuste (narrow), amplias (wide).

**Transformaciones de un solo RDD**
Operaciones que se aplican a un único RDD.

Ejemplos: `map()`, `filter()`, `flatMap()`, `distinct()`, `sample()`, etcétera.

**Transformaciones entre dos RDD**
Operaciones que involucran dos RDD.

Ejemplos: `union()`, `intersection()`, `subtract()`, `cartesian()`, etcétera.

![](./media/image1.png)

### Trabajando con esquemas (schema)

Los esquemas en RDD proporcionan una forma de definir la estructura de
los datos dentro de un RDD, especificando los nombres y tipos de las
columnas. Esto permite a PySpark optimizar el procesamiento de los datos y realizar operaciones más eficientes.

**¿Por qué usar esquemas en RDD?**

-   Mejor rendimiento. Al definir un esquema, PySpark puede optimizar
    las consultas y transformaciones, lo que se traduce en un
    procesamiento más rápido y eficiente.

-   Validación de datos. Los esquemas permiten validar los datos al
    momento de cargarlos o procesarlos, asegurando que cumplan con la
    estructura definida.

-   Mayor legibilidad. Al tener una estructura clara y definida, el
    código es más fácil de entender y mantener.

-   Integración con otras herramientas. Los esquemas facilitan la
    integración con otras herramientas y librerías, como DataFrames y
    SQL.

## Instrucciones

### Tarea 1. Crear un RDD con esquema a partir de un archivo CSV

- Inicia la sesión de PyCharm.

```
pycharm-community
```

Crea un archivo Python e introduce el siguiente código. Se ajusta
la ruta al archivo `Customers.cvs`.

```
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("CSVConEsquema").getOrCreate()

#Se describe la estructura a leer

schema = StructType(\[

StructField("ID", StringType(), True),

StructField("Name", StringType(), True),

StructField("Address", StringType(), True),

StructField("Gender", StringType(), True),

StructField("Status", StringType(), True)

\])

rdd =
spark.sparkContext.textFile("/home/miguel/data/Model/Customers.csv")

#Se elimina el encabezado

encabezado = rdd.zipWithIndex().filter(lambda x: x\[1\] \> 0).map(lambda
x: x\[0\])

#Se convierte el RDD a DataFrame

rdd_con_esquema = encabezado.map(lambda linea:
linea.split(",")).toDF(schema)

rdd_con_esquema.show()

rdd_con_esquema.printSchema()
```

![](./media/image2.png)

![](./media/image3.png)


En este ejemplo, primero, se carga el archivo CSV utilizando `textFile()`.
Luego, se omite la primera línea si contiene encabezados. Después, se
divide cada línea en campos con `split(",")`. Finalmente, se crea el RDD con esquema utilizando `toDF(schema)`.

-   Es importante asegurarse de que los tipos de datos en el archivo CSV coinciden con los tipos de datos definidos en el esquema.

-   Si el archivo CSV contiene encabezados, es necesario omitirlos al
    crear el RDD con esquema.

-   Si el archivo CSV contiene valores nulos, se pueden manejar
    utilizando la opción nullable=True en la definición del esquema.



**Leer desde un archivo parquet con esquema**

```
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType,
IntegerType, LongType

spark = SparkSession.builder \\

.master("local") \\

.appName("RDDdesdeParquet") \\

.getOrCreate()

sc = spark.sparkContext

esquema = StructType(\[

StructField("Precio", LongType(), True),

StructField("Area", LongType(), True),

StructField("Recamaras", IntegerType(), True),

StructField("Baños", IntegerType(), True),

StructField("Historias", LongType(), True),

StructField("CallePrincipal", StringType(), True),

StructField("CuartoInvitados", StringType(), True),

StructField("Sotano", StringType(), True),

StructField("AguaCaliente", StringType(), True),

StructField("AireAcondicionado", StringType(), True),

StructField("Estacionamiento", IntegerType(), True),

StructField("AreaPreferida", StringType(), True),

StructField("EstadoMobiliario", StringType(), True)

\])

#Cargar el archivo parquet

df = spark.read.parquet("/home/miguel/data/house-price.parquet")

#Convertir el DataFrame a rdd

rdd=df.rdd

#Convertir el DataFrame a rdd

rdd_esquema = rdd.toDF(esquema).rdd

#Alguna actividad con el rdd

for row in rdd_esquema.collect():

print(row)

df.show(5, 0)

df.printSchema()
```

![](./media/image4.png)

![](./media/image5.png)
![](./media/image6.png)

**Recomendaciones**

-   Utilizar DataFrames cuando sea posible. Si bien es posible crear
    RDD con esquemas desde archivos parquet, se recomienda utilizar
    DataFrames directamente, ya que ofrecen un mejor rendimiento y
    funcionalidades para el procesamiento de datos estructurados.

-   Definir esquemas explícitos. Si necesitas un control preciso sobre
    el esquema de los datos, define un esquema explícito utilizando
    StructType y StructField.

### Tarea 2. Aplicar transformaciones

**Características de las transformaciones**

-   **Lazy Evaluation (Perezosas).** Las transformaciones no se ejecutan hasta que se llama a una acción (como `collect()`, `count()`,
    `saveAsTextFile()`, etcétera). Esto permite optimizar el plan de
    ejecución.

-   **Inmutabilidad.** Los RDD son inmutables, lo que significa que las
    transformaciones no modifican el RDD original, sino que generan uno
    nuevo.

-   **Lineage (Linaje).** PySpark mantiene un registro de las
    transformaciones aplicadas a un RDD, lo que permite recalcular datos en caso de fallos.

Procesa un conjunto de frases para contar la frecuencia de
cada palabra:

```
from pyspark import SparkContext

sc = SparkContext("local", "ConteoPalabras")

\# Crear un RDD a partir de una lista de frases

frases = \["Error: usuario magarcia2 no existe", "Error: usuario
magarcia2 llego al limite de accesos en dragonlair", "Aviso: servidor
dragonlair degradado"\]

rdd = sc.parallelize(frases)

\# Dividir frases en palabras

rdd_palabras = rdd.flatMap(lambda x: x.split(" "))

\# Convertir a mayúsculas

rdd_palabras_mayusculas = rdd_palabras.map(lambda x: x.upper())

\# Contar frecuencia

rdd_frecuencia = rdd_palabras_mayusculas.map(lambda x: (x,
1)).reduceByKey(lambda a, b: a + b)

\# Ejecutar acción para obtener resultados resultado =
rdd_frecuencia.collect() print(resultado)
```

![](./media/image7.png)

![](./media/image8.png)

### Tarea 3. Aplicando transformaciones comunes

**Función `map`**

La función `map` es una de las transformaciones más comunes y
poderosas que se pueden aplicar a un RDD. Toma una función como
argumento y la aplica a cada elemento del RDD, devolviendo un nuevo RDD
con los resultados.

La función que pasas a `map` se aplica a cada elemento del RDD, uno por
uno. Devuelve un nuevo RDD donde cada elemento es el resultado de
aplicar la función al elemento original.

```
salida= entrada.map(funcion())
```

La función `map` toma como argumento una función (por ejemplo, una función de Python o una lambda), que se aplica a cada elemento del RDD.

Devuelve un nuevo RDD donde cada elemento es el resultado de aplicar la
función al elemento original.

**Convertir todos los elementos de un RDD a mayúsculas**

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Transformaciones RDD")

rdd = sc.parallelize(\["hola", "mundo", "pyspark"\])

rdd_mayusculas = rdd.map(lambda x: x.upper())

print(rdd_mayusculas.collect())
```

![](./media/image9.png)

![](./media/image10.png)

**Transformar tuplas**

Se tiene un RDD de tuplas que representan nombres y edades y se desea
incrementar la edad de cada persona en 2.

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Transformar tuplas RDD")

\# Crear un RDD de tuplas

rdd = sc.parallelize(\[("Alicia", 25), ("Bernardo", 30), ("Carolina",
28)\])

\# Aplicar la función map para incrementar la edad en 2

rdd_transformado = rdd.map(lambda x: (x\[0\], x\[1\] + 2))

\# Mostrar el resultado

print(rdd_transformado.collect())
```

![](./media/image11.png)

![](./media/image12.png)

**Transformar cadenas de texto**

Convertir cada cadena a mayúsculas.

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Transformar Texto en RDD")

\# Crear un RDD de tuplas

rdd = sc.parallelize(\["Alicia","Bernardo","Carolina"\])

\# Aplicar la función map para convertir a mayúsculas

rdd_transformado = rdd.map(lambda x: x.upper())

\# Mostrar el resultado

print(rdd_transformado.collect())
```

![](./media/image13.png)

![](./media/image14.png)

**Transformaciones más complejas**

Puedes usar `map` para realizar transformaciones más complejas, como
extraer información de un formato de datos.

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Transformar Texto en RDD")

\# Crear un RDD de cadenas en formato "nombre:edad"

rdd = sc.parallelize(\["Alice:25", "Bob:30", "Cathy:28"\])

\# Aplicar la función map para extraer nombre y edad

rdd_transformado = rdd.map(lambda x: (x.split(":")\[0\],
int(x.split(":")\[1\])))

\# Mostrar el resultado

print(rdd_transformado.collect())
```

![](./media/image15.png)

![](./media/image16.png)

**Función `flatMap`**

Transformación que se aplica a un RDD y devuelve un nuevo
RDD. A diferencia de `map`, puede "aplanar" los resultados.

-   **`map`**. Aplica una función a cada elemento del RDD y devuelve un
    nuevo RDD con los resultados. Si la función devuelve una lista o
    secuencia, cada elemento de esa lista se mantiene como un elemento
    separado en el nuevo RDD.

-   **`flatMap`**. Aplica una función a cada elemento del RDD y devuelve un nuevo RDD con los resultados. Si la función devuelve una lista o secuencia, `flatMap` "aplana" esa lista, de modo que los elementos individuales de la lista se convierten en elementos separados en el nuevo RDD.

`flatMap` es útil cuando tienes un RDD donde cada elemento contiene una
colección (como una lista) y deseas crear un nuevo RDD donde cada
elemento sea un elemento individual de esas colecciones.

Si aplicas `map` para dividir cada frase en palabras, obtendríamos un
RDD donde cada elemento es una lista de palabras:

```
palabras_map = rdd.map(lambda frase: frase.split())

\# Resultado: \[\['Hola', 'mundo'\], \['Python', 'es', 'genial'\],
\['Spark', 'es', 'poderoso'\]\]
```

Pero si usas `flatMap`, obtendrás un RDD donde cada palabra es un
elemento individual:

```
palabras_flat = rdd.flatMap(lambda frase: frase.split())
```

**Extrayendo términos**

```
from pyspark import SparkContext

sc = SparkContext("local", "Aplicando flatMap")

frases = \["Error de aplicación web",\\

"Aviso de recursos al límite",\\

"Error de seguridad en aplicación local"\]

rdd = sc.parallelize(frases)

palabras_map = rdd.map(lambda frase: frase.split())

palabras_flat = rdd.flatMap(lambda frase: frase.split())

print("Resultado con map:")

for palabras in palabras_map.collect():

print(palabras)

print("\\nResultado con flatMap:")

for palabra in palabras_flat.collect():

print(palabra)
```

![](./media/image17.png)

![](./media/image18.png)

![](./media/image19.png)

**Función `filter`**

Transformación que se utiliza para seleccionar elementos
de un RDD que cumplen con una condición específica. La función `filter`
toma una función (o una expresión lambda) que devuelve `True` o `False` para cada elemento del RDD. Solo los elementos que devuelven `True` se incluyen en el nuevo RDD resultante.

**Sintaxis de filter**

```
nuevo_rdd = rdd.filter(función)
```

`rdd`: el RDD original.

`función`: una función que toma un elemento del RDD y devuelve `True` o `False`.

`nuevo_rdd`: el RDD resultante que contiene solo los elementos que cumplen la condición.

**Filtrar números pares**

```
from pyspark import SparkContext

sc = SparkContext("local",\\
```

**"Filtrar RDD**

```
rdd = sc.parallelize(\[1, 2, 3, 4, 5, 6\])

rdd_pares = rdd.filter(lambda x: x % 2 == 0)

**print(rdd_pares.collect())**
```

![](./media/image20.png)

![](./media/image21.png)

**Filtrar filas basadas en una condición**

Se tiene un RDD de tuplas que representan personas con su nombre y edad y se desea filtrar solo las personas mayores de 25 años.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "FilterEjemplo")

\# Crear un RDD de personas (nombre, edad)

rdd = sc.parallelize(\[("Alice", 25), ("Bob", 30), ("Cathy", 28),
("David", 22)\])

\# Filtrar personas mayores de 25 años

rdd_mayores = rdd.filter(lambda x: x\[1\] \> 25)

\# Mostrar el resultado

print("Personas mayores de 25 años:")

print(rdd_mayores.collect())
```

![](./media/image22.png)

![](./media/image23.png)

**Filtrar usando una función definida**

En lugar de usar una expresión lambda, se puede definir una función para realizar el filtrado. Esto es útil cuando la lógica de filtrado es más compleja.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "FilterEjemplo")

\# Crear un RDD de personas (nombre, edad)

rdd = sc.parallelize(\[("Alice", 25), ("Bob", 30), ("Cathy", 28),
("David", 22)\])

\# Definir una función de filtrado

def es_mayor_de_25(persona):

nombre, edad = persona

return edad \> 25

\# Filtrar usando la función

rdd_mayores = rdd.filter(es_mayor_de_25)

\# Mostrar el resultado

print("Personas mayores de 25 años:")

print(rdd_mayores.collect())
```

![](./media/image24.png)

![](./media/image25.png)

**Filtrar basado en múltiples condiciones**

Se pueden combinar múltiples condiciones usando operadores lógicos como
`and`, `or`, etcétera.

```
from pyspark import SparkContext

sc = SparkContext("local", "FilterEjemplo")

\# Crear un RDD de personas (nombre, edad, ciudad)

rdd = sc.parallelize(\[

("Alice", 25, "Medellin"),

("Bob", 30, "Bogotá"),

("Cathy", 28, "Bogotá"),

("David", 22, "Cali"),

("Ernesto", 21, "Bogotá"),

("Fernanda", 20, "Medellin"),

\])

\# Filtrar personas que viven en Bogotá y tienen más de 25 años

rdd_filtrado = rdd.filter(lambda x: x\[2\] == "Bogotá" and x\[1\] \> 25)

\# Mostrar el resultado

print("Personas que viven en Bogotá y tienen más de 25 años:")

print(rdd_filtrado.collect())
```

![](./media/image26.png)

![](./media/image27.png)

**Función `distinct`**

Se utiliza para eliminar duplicados en un RDD. Esta función devuelve un nuevo RDD que contiene solo los elementos únicos del RDD original. Es una operación útil cuando necesitas eliminar filas o registros repetidos en un conjunto de datos.

```
salida = entrada.distinct()
```

-   `entrada`: un RDD con elementos que pueden contener duplicados.

-   `salida`: un nuevo RDD donde cada elemento aparece solo una vez (sin duplicados).

La función `distinct()` realiza un **shuffle** internamente para agrupar y
eliminar los duplicados. Esto puede ser costoso en términos de
rendimiento, especialmente con grandes volúmenes de datos

**Obtener elementos únicos**

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Distinct en RDD")

\# Crear un RDD con elementos duplicados

rdd = sc.parallelize(\[1, 2, 3, 4, 2, 3, 5, 6, 1\])

\# Aplicar la función distinct() para eliminar duplicados

rdd_distinct = rdd.distinct()

\# Mostrar el RDD resultante

print("RDD original:", rdd.collect())

print("RDD sin duplicados:", rdd_distinct.collect())
```

![](./media/image28.png)

![](./media/image29.png)

**Eliminar filas duplicadas**

En un RDD de tuplas, `distinct()` eliminará las tuplas duplicadas en su totalidad.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "DistinctTuplesExample")

\# Crear un RDD de tuplas con duplicados

rdd = sc.parallelize(\[("Alice", 25), ("Bob", 30), ("Alice", 25),
("Cathy", 28)\])

\# Aplicar distinct() para eliminar tuplas duplicadas

rdd_distinct = rdd.distinct()

\# Mostrar el RDD resultante

print("RDD original:", rdd.collect())

print("RDD sin duplicados:", rdd_distinct.collect())
```

![](./media/image30.png)

![](./media/image31.png)

**Eliminar duplicados basados en una "columna"**

Si deseas eliminar duplicados basados en una "columna" específica (un
elemento de la tupla), puedes usar `map` para seleccionar esa columna
y luego aplicar `distinct()`. Después, puedes unir los datos
originales con los elementos únicos.

```
from pyspark import SparkContext

sc = SparkContext("local", "DistinctColumnExample")

\# Crear un RDD de tuplas

rdd = sc.parallelize(\[("Alice", 25), ("Bob", 30), ("Alice", 25),
("Cathy", 28)\])

\# Seleccionar la columna "nombre" (primer elemento de la tupla) y
aplicar distinct()

nombres_unicos = rdd.map(lambda x: x\[0\]).distinct()

\# Mostrar los nombres únicos

print("Nombres únicos:", nombres_unicos.collect())

\# Filtrar el RDD original para mantener solo las filas con nombres
únicos

rdd_filtrado = rdd.map(lambda x: (x\[0\],
x)).join(nombres_unicos.map(lambda x: (x, 1))).map(lambda x:
x\[1\]\[0\])

\# Mostrar el RDD filtrado

print("RDD filtrado:", rdd_filtrado.collect())
```

![](./media/image32.png)

**En este ejemplo:**

-   Usa `map(lambda x: x\[0\])` para seleccionar solo la primera
    columna (el nombre).

-   Elimina los duplicados en la columna seleccionada con `distinct()`.

-   Usa `join` para combinar el RDD original con los nombres únicos.

-   Finalmente, mapea el resultado para obtener solo las filas
    originales sin duplicados.

### Función `union`

Se utiliza para combinar dos RDD en uno solo. Esta función retorna un nuevo RDD que contiene todos los elementos
de ambos RDD originales. Es importante destacar que `union` no elimina
duplicados; si los RDD tienen elementos repetidos, estos se mantendrán
en el RDD resultante.

```
rdd_resultante = rdd1.union(rdd2)

-   rdd1: El primer RDD.

-   rdd2: El segundo RDD.

-   rdd_resultante: Un nuevo RDD que contiene todos los elementos de
    rdd1 y rdd2.

**Unir dos listas.**

from pyspark import SparkContext

sc = SparkContext("local",\\

"Union de RDD")

rdd1 = sc.parallelize(\[1, 2, 3, 4\])

rdd2 = sc.parallelize(\[3, 4, 5, 6\])

\# Combinar los RDDs usando union

rdd_union = rdd1.union(rdd2)

\# Mostrar el resultado

print("RDD combinado:")

print(rdd_union.collect())
```

![](./media/image33.png)

![](./media/image34.png)

**RDD de tuplas**

También puedes usar union con RDDs que contienen estructuras más
complejas, como tuplas.

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Union de RDD")

\# Crear dos RDDs de tuplas

rdd1 = sc.parallelize(\[("Alice", 25), ("Bob", 30)\])

rdd2 = sc.parallelize(\[("Bob", 30), ("Cathy", 28)\])

\# Combinar los RDDs usando union

rdd_union = rdd1.union(rdd2)

\# Mostrar el resultado

print("RDD combinado:")

print(rdd_union.collect())
```

![](./media/image35.png)
![](./media/image36.png)

`union` no elimina duplicados. Si necesitas eliminar elementos repetidos,
puedes usar la función `distinct` después de aplicar `union`.

```
rdd_sin_duplicados = rdd_union.distinct()

print(rdd_sin_duplicados.collect())
```
union no garantiza un orden específico en el RDD resultante. Los
elementos se combinan en el orden en que se encuentran en los RDD
originales.

### Función `intersection`

`Intersection` permite encontrar los elementos comunes entre dos
RDD. Devuelve un nuevo RDD que contiene solo los elementos que están
presentes en ambos RDDs originales.

-   Ambos RDD deben contener elementos del mismo tipo para que la
    operación sea válida.

-   `intersection` implica un `shuffle` (reorganización de datos entre
    particiones), lo que puede ser costoso en términos de rendimiento,
    especialmente con grandes volúmenes de datos.

-   El RDD resultante no contendrá elementos duplicados, incluso si los
    RDDs originales los tenían.

rdd1.intersection(rdd2)

-   rdd1: Primer RDD.

-   rdd2: Segundo RDD.

**Intersección de dos listas**

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Transformaciones RDD")

rdd1 = sc.parallelize(\[1, 2, 3, 4, 5, 6\])

rdd2 = sc.parallelize(\[1, 3, 4, 5,7\])

rdd_interseccion = rdd1.intersection(rdd2)

print(rdd_interseccion.collect())
```

![](./media/image37.png)

![](./media/image38.png)

**Intersección con cadenas de texto**

También puedes usar `intersection` con RDD que contengan cadenas de
texto.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "Ejemplo intersection")

\# Crear dos RDDs de ejemplo con cadenas de texto

rdd1 = sc.parallelize(\["Medellin", "Cali", "Pereira", "Cordoba"\])

rdd2 = sc.parallelize(\["Barranquilla", "Medellin", "Cartagena",
"Pereira"\])

\# Encontrar la intersección entre los dos RDDs

rdd_intersection = rdd1.intersection(rdd2)

\# Mostrar el resultado

print("Intersección de rdd1 y rdd2:")

print(rdd_intersection.collect())
```

![](./media/image39.png)

![](./media/image40.png)

### Función substract

**subtract -** se utiliza para obtener los elementos que están en un RDD
pero no en otro. Realiza una diferencia de conjuntos entre dos RDDs. El
resultado es un nuevo RDD que contiene solo los elementos que están en
el primer RDD y no en el segundo.

resultado_rdd = rdd1.subtract(rdd2)

-   `rdd1`: el RDD del cual se quieren extraer los elementos.

-  ` rdd2`: el RDD cuyos elementos se quieren excluir del primer RDD.

-   `resultado_rdd`: un nuevo RDD que contiene los elementos de `rdd1` que
    no están en `rdd2`.

**Restar dos listas**

```
from pyspark import SparkContext

sc = SparkContext("local", "Substract con RDD")

rdd1 = sc.parallelize(\[1, 2, 3, 4\])

rdd2 = sc.parallelize(\[3, 4, 5, 8\])

rdd_diferencia = rdd1.subtract(rdd2)

print(rdd_diferencia.collect())
```

![](./media/image41.png)

![](./media/image42.png)

**Usando tuplas**

La función subtract también funciona con RDDs que contienen tuplas o
estructuras más complejas.

```
from pyspark import SparkContext

sc = SparkContext("local", "Subtract tuplas")

\# Crear los RDDs con tuplas

rdd1 = sc.parallelize(\[("Alice", 25), ("Bob", 30), ("Cathy", 28)\])

rdd2 = sc.parallelize(\[("Bob", 30), ("David", 40)\])

\# Usar subtract para obtener las tuplas de rdd1 que no están en rdd2

resultado_rdd = rdd1.subtract(rdd2)

\# Mostrar el resultado

print("Tuplas en rdd1 que no están en rdd2:")

print(resultado_rdd.collect())
```

`Subtract` compara los elementos de los RDD de manera exacta. Para tuplas
o estructuras complejas, todos los elementos de la tupla deben coincidir
para que se considere un duplicado.

### Función `cartesian`

Se utiliza para calcular el producto cartesiano entre
dos RDD. El producto cartesiano de dos conjuntos (o RDD) es un
conjunto de pares ordenados donde el primer elemento pertenece al primer
RDD y el segundo elemento pertenece al segundo RDD. 
Ejemplo: producto
cartesiano de dos listas.

Toma dos RDD, `RDD1` y `RDD2`.

Devuelve un nuevo RDD que contiene todas las combinaciones posibles de
elementos entre `RDD1` y `RDD2`.

El tamaño del RDD resultante es el producto de los tamaños de `RDD1` y
`RDD2`. Por ejemplo, si `RDD1` tiene 3 elementos y `RDD2` tiene 2 elementos,
el RDD resultante tendrá 3×2=6 elementos.

```
from pyspark import SparkContext

sc = SparkContext("local",\\

"Producto cartesiano RDD")

\# Crear dos RDDs

rdd1 = sc.parallelize(\[1, 2, 3\])

rdd2 = sc.parallelize(\["A", "B"\])

\# Calcular el producto cartesiano

cartesian_rdd = rdd1.cartesian(rdd2)

\# Mostrar el resultado

print("Producto cartesiano:")

print(cartesian_rdd.collect())
```

El producto cartesiano puede generar un RDD muy grande, ya que su tamaño
es el producto de los tamaños de los dos RDDs originales.

Por ejemplo, si `rdd1` tiene 1,000 elementos y `rdd2` tiene 1,000 elementos,
el RDD resultante tendrá 1,000,000 de elementos. Esto puede consumir
mucha memoria y tiempo de procesamiento.

El producto cartesiano es útil en casos donde necesitas comparar o
combinar todos los elementos de un conjunto con todos los elementos de
otro conjunto.

### Función `groupByKey`

Es una transformación que se aplica a RDD que
contienen pares clave-valor. Su propósito es agrupar todos los valores
que comparten la misma clave en una sola colección. El resultado es un
nuevo RDD donde cada clave única está asociada a un iterable de todos
los valores correspondientes a esa clave.

```
from pyspark import SparkContext

sc = SparkContext("local", "groupByKey_example")

data = \[

("Juan", "Matemáticas", 8),

("María", "Ciencias", 9),

("Juan", "Física", 7),

("Pedro", "Matemáticas", 6),

("María", "Química", 8),

("Pedro", "Física", 9)

\]

rdd = sc.parallelize(data)

grouped_rdd = rdd.map(lambda x: (x\[0\], (x\[1\], x\[2\]))).groupByKey()

for student, grades in grouped_rdd.collect():

print(f"Estudiante: {student}")

for subject, grade in grades:

print(f" {subject}: {grade}")
```
![](./media/image43.png)

![](./media/image44.png)

-   `groupByKey` puede ser costoso en términos de rendimiento, ya que
    implica mezclar todos los datos con la misma clave en un solo lugar.
    Si necesitas realizar operaciones de agregación (como sumas o
    promedios), es más eficiente usar reduceByKey o aggregateByKey.

-   El resultado de `groupByKey` es un RDD donde los valores son
    iterables.

-   Si necesitas realizar una operación de reducción (como suma,
    promedio, máximo, etc.) sobre los valores agrupados, `reduceByKey` es
    una opción más eficiente.

-   `aggregateByKey` es similar a `reduceByKey`, pero ofrece más
    flexibilidad para inicializar y combinar los valores.

### Función `reduceByKey`

Transformación que se aplica a RDD que contienen
pares clave-valor. Su objetivo es combinar los valores que comparten la
misma clave, utilizando una función de reducción. Esta función debe ser
asociativa y conmutativa para garantizar resultados consistentes.

-   `reduceByKey` agrupa todos los pares clave-valor que tienen la misma
    clave.

-   Para cada grupo de valores con la misma clave, se aplica la función
    de reducción para obtener un único valor combinado.

-   Se crea un nuevo RDD donde cada clave única está asociada al valor
    combinado resultante.

**Información de ventas de diferentes productos**

```
from pyspark import SparkContext

sc = SparkContext("local", "ReduceByKey_Example")

data = \[

("ProductoA", 10),

("ProductoB", 5),

("ProductoA", 15),

("ProductoC", 8),

("ProductoB", 12)

\]

rdd = sc.parallelize(data)

total_ventas = rdd.reduceByKey(lambda a, b: a + b)

\# Mostrar el resultado

for producto, ventas in total_ventas.collect():

print(f"Producto: {producto}, Ventas totales: {ventas}")
```

En este ejemplo:

-   `rdd.reduceByKey(lambda a, b: a + b)`: aplica la función `reduceByKey`
    al RDD. La función `lambda a, b: a + b` es la función de reducción que
    suma los valores.

-   La función de reducción debe ser asociativa y conmutativa. Por
    ejemplo, la suma `(a + b)` lo es, pero la resta `(a - b)` no lo es.

-   `reduceByKey` es una operación eficiente para grandes conjuntos de
    datos, ya que realiza la reducción de forma distribuida.

-   Es importante recordar que `reduceByKey` opera sobre RDD de pares
    clave-valor.

**Además de la suma, se pueden usar otras funciones de reducción como:**

-   Máximo: `lambda a, b: max(a, b)`.

-   Mínimo: `lambda a, b: min(a, b)`.

-   Multiplicación: `lambda a, b: a \* b`.

### Función `sortByKey`

Se utiliza en RDD que contienen pares clave-valor. Su
propósito es ordenar los elementos del RDD basándose en las claves. Por
defecto, ordena de forma ascendente, pero esto se puede cambiar.

`sortByKey(ascending=True, numPartitions=None, keyfunc=lambda x: x)`

-   `ascending`: booleano. `True` para orden ascendente (predeterminado),
    `False` para descendente.

-   `numPartitions`: entero. Número de particiones para el RDD resultante.

-   `keyfunc`: función que se aplica a las claves antes de la ordenación.
  
```
from pyspark import SparkContext

sc = SparkContext("local", "SortByKey")

data = \[("manzana", 10), ("banana", 20), ("naranja", 5), ("uva", 15)\]

rdd = sc.parallelize(data)

sorted_rdd = rdd.sortByKey()

for fruta, cantidad in sorted_rdd.collect():

print(f"{fruta}: {cantidad}")

from pyspark import SparkContext

sc = SparkContext("local", "SortByKey")

data = \[("manzana", 10), ("banana", 20), ("naranja", 5), ("uva", 15)\]

rdd = sc.parallelize(data)

sorted_rdd = rdd.sortByKey(ascending=False)

for fruta, cantidad in sorted_rdd.collect():

print(f"{fruta}: {cantidad}")
```

-   `sortByKey` solo funciona en RDD de pares clave-valor.

-   La ordenación se realiza dentro de cada partición del RDD. Si
    necesitas una ordenación global, puedes usar `repartition(1)` para
    tener una sola partición.

-   Si tienes claves duplicadas, sortByKey no las agrupa, simplemente
    las ordena según su orden de aparición.

### Filtrado de columnas

**Crear RDD solo con campos seleccionados**

En PySpark, tienes varias formas de crear un RDD con solo algunos campos
de un archivo de origen.

La forma más sencilla y eficiente de seleccionar solo algunos campos es
usar la función **select**.

Nota la estructura del archivo Sales.csv.

![](./media/image45.png)

Tienes más de 10 campos y no requieres en este momento trabajar con
todos, así que solo selecciona los campos necesarios.

```
from pyspark.sql import SparkSession

spark =
SparkSession.builder.appName("SeleccionarCamposRDD").getOrCreate()

\# Leer el archivo de origen

df = spark.read.csv("/home/miguel/data/Sales.csv", header=True,
inferSchema=True)

\# Seleccionar solo los campos deseados

campos = \["SalesOrderNumber", "OrderDate", "Customer","Country"\]

nuevo_df = df.select(\*campos)

\# Convertir el DataFrame resultante a RDD

rdd = nuevo_df.rdd

\# Trabajar con el RDD

for row in rdd.collect():

print(row)
```

![](./media/image46.png)

![](./media/image47.png)

**Usando `map` para seleccionar campos**

Otra alternativa es utilizar la función `map` para transformar cada
elemento del RDD.

```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SeleccionarCamposRDD")\\

.getOrCreate()

\# Crear un RDD

data = \[("A", 1, "X", True), ("B", 2, "Y", False), ("C", 3, "Z",
True)\]

rdd_original = spark.sparkContext.parallelize(data)

\# Seleccionar solo los campos deseados usando map

indices_deseados = \[0, 2\] \# Índices de los campos a seleccionar (0 =
"A", 2 = "X")

rdd = rdd_original.map(lambda row: tuple(row\[i\] for i in
indices_deseados))

\# Trabajar con el RDD

for row in rdd.collect():

print(row)
```

### Convertir tipos de datos en un RDD

Los RDD no tienen un esquema definido, por lo que no tienen columnas
con tipos de datos específicos. Sin embargo, se puede manipular los
datos dentro de un RDD para convertir tipos de datos durante el
procesamiento.

```
from pyspark import SparkContext

\# Inicializar SparkContext

sc = SparkContext("local", "ConvertirTiposRDD")

\# Crear un RDD de ejemplo

rdd = sc.parallelize(\[("Alejandro", "25"), ("Betriz", "30"), ("Carmen",
"28")\])

\# Mostrar el RDD original

print("RDD original:")

print(rdd.collect())

\# Convertir la segunda columna (edad) de string a int

rdd_converted = rdd.map(lambda x: (x\[0\], int(x\[1\])))

\# Mostrar el RDD convertido

print("RDD convertido:")

print(rdd_converted.collect())
```

**En este ejemplo:**

`map`: esta transformación aplica una función a cada elemento del RDD.
En este caso, la función es una lambda que toma una tupla `x` y devuelve
una nueva tupla donde el primer elemento `(x\[0\])` permanece igual y el
segundo elemento `(x\[1\])` se convierte a un entero usando `int()`.

**`Lambda function: lambda x: (x\[0\], int(x\[1\]))`:**

**`x\[0\]`:** conserva el primer elemento de la tupla (el nombre).

**`int(x\[1\])`:** convierte el segundo elemento de la tupla (la edad) a
un entero.

![](./media/image48.png)

![](./media/image49.png)

**Consideraciones importantes**

-   **Manejo de errores.** Si algunos valores no se pueden convertir
    (por ejemplo, si una cadena no es un número válido), se lanzará una
    excepción. Puedes manejar esto usando bloques try-except dentro de
    la función map.

-   **Rendimiento.** Las operaciones de transformación en RDDs son
    perezosas `(lazy)`, lo que significa que no se ejecutan hasta que se
    llama a una acción (como `collect()`). Esto permite que Spark optimice
    el procesamiento.

-   **Uso de DataFrames.** Si estás trabajando con datos estructurados y
    necesitas convertir tipos de datos con frecuencia, es recomendable
    usar DataFrames en lugar de RDD. Los `DataFrames` tienen soporte
    integrado para esquemas y conversiones de tipos de datos.

