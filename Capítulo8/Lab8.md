# Práctica 8. Monitoreo y optimización de PySpark

## **Objetivo de la práctica:** 

Al finalizar la práctica serás capaz de:
- Identificar y aplicar mejores prácticas de desempeño y el uso de Catalyst Optimizer.

## **Duración aproximada:** 
- 60 minutos.

## **Prerequisitos:**
-   Acceso al ambiente Linux (credenciales provistas en el curso) o Linux local con interfaz gráfica.
-   Tener los archivos de datos.
-   Completar el laboratorio 1.

## **Contexto:**

El monitoreo del código en PySpark implica supervisar el rendimiento de las aplicaciones, identificar cuellos de botella y asegurarte de que los recursos se utilicen de manera eficiente. PySpark proporciona varias herramientas para monitorear el rendimiento

## **Instrucciones:** Revisar el documento con las mejores recomendaciones

## Tarea 1: Monitoreo del Código en PySpark

El monitoreo del código en PySpark implica supervisar el rendimiento de las aplicaciones, identificar cuellos de botella y asegurarte de que los recursos se utilicen de manera eficiente. PySpark proporciona varias herramientas para monitorear el rendimiento:

### **Herramientas de monitoreo**

**a) Spark UI**

La interfaz de usuario de Spark (Spark UI) es una herramienta visual que te permite monitorear el progreso de las tareas, el uso de recursos y el rendimiento de las consultas. Puedes acceder a ella a través de la URL proporcionada en los logs de Spark (normalmente en ***http://&lt;driver-node&gt;:4040***).

**Ejemplo**: Después de ejecutar un trabajo en PySpark, abre la Spark UI para ver:

-   El plan de ejecución de las consultas.
-   El tiempo de ejecución de cada etapa.
-   El uso de memoria y CPU.


**b) Logs de Spark**

Los logs de Spark proporcionan información detallada sobre el progreso de las tareas, errores y advertencias. Puedes configurar el nivel de logging para obtener más o menos detalle.

```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MonitoringExample").getOrCreate()
spark “sparkContext.setLogLevel("INFO") \# Niveles: ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF
```

**c) Métricas de Spark**

1. Spark expone métricas a través de sistemas como Prometheus, Graphite o JMX. Estas métricas incluyen:

-   Tiempo de ejecución de tareas.
-   Uso de memoria y CPU.
-   Número de registros procesados.

**Ejemplo:** Configurar Spark para enviar métricas a un sistema externo:

2. Aplicación de mejores prácticas en PySpark

Para escribir código eficiente en PySpark, es importante seguir mejores prácticas. Aquí algunas de las más importantes:

## **Evitar operaciones costosas:**

**1. Evitar collect()** 
Traer todos los datos al driver puede causar problemas de memoria. En su lugar, usa take() o show().

```
# Incorrecto
data = df.collect() # Trae todos los datos al driver

# Correcto
data = df.take(10) # Trae solo 10 filas
```

2. **Evitar shuffle innecesario:** Operaciones como groupBy, join o distinct pueden causar un shuffle, que es costoso. Minimiza su uso.

```
# Incorrecto
df.groupBy("columna").count().show()

# Correcto (si es posible)
df.select("columna").distinct().count()
```

3. **Usar el esquema correcto**
Definir el esquema de tus DataFrames explícitamente para evitar inferencias costosas.

```
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField(“nombre", StringType(), ),
    Struc“Field(”edad", IntegerType(), ),
])

df = spark.read.schema(schema).csv("data.csv")
```

**4. Subir a cache DataFrames cuando sea necesario**
Mantener en cache DataFrames si se reutilizan en múltiples operaciones.

```
df.cache() # Almacena el DataFrame en memoria
df.count() # Forzar caché
```

**5. Particionar datos adecuadamente**
Asegúrate de que los datos estén bien particionados para paralelizar el procesamiento.

<!-- -->

`df = df.repartition(100) \# Reparticion el DataFrame en 100 particiones`

## **Uso del Catalyst Optimizer**

**Catalyst Optimizer** es el motor de optimización de Spark SQL. Transforma las consultas SQL o las operaciones de DataFrames en un plan de ejecución optimizado.

**Funcionamiento del Catalyst Optimizer**

-   **Análisis**: Verifica la validez de las columnas y tablas.
-   **Optimización lógica:** Aplica reglas para simplificar la consulta (por ejemplo, eliminar filtros innecesarios).
-   **Planificación física**: Genera un plan de ejecución eficiente (por ejemplo, selecciona el tipo de join más adecuado).
-   **Generación de código**: Convierte el plan en código ejecutable.

**Ejemplo de optimización**

```
df = spark.read.csv("data.csv")

df_filtered = df.filter(df["“dad"] &gt; ”0)
df_grouped = df_filtered.g”oupB”("ciudad").count()
df_grouped.show()
```

**Catalyst Optimizer:**

- Combina **filter** y **groupBy** en una sola operación.
- Selecciona el algoritmo de **join** más eficiente.
- Genera código optimizado para ejecutar la consulta.

**Ver el plan de ejecución**
Puedes ver el plan de ejecución generado por Catalyst usando explain():

```
df_grouped.explain()

== Physical Plan ==

*(2) HashAggregate(keys=[ciudad], functions=[count(1)])

+- Exchange hashpartitioning(ciudad, 200)

+- *(1) HashAggregate(keys=[ciudad], ctions=[partial_count(1)])

+- *(1) Filter (edad &gt; 30)

FileScan csv [ciudad,edad] Balse, Format: CSV, ...
```

**Optimización manual**

Puedes guiar al optimizador con acciones como:

- Broadcast Join: Para unir un DataFrame pequeño con uno grande.

```
from pyspark.sql.functions import broadcast
df_joined = df1.join(broadcast(df2), "id")
```

- **Evitar operaciones costosas**: Como shuffle o full scans.

Ejemplo que combina monitoreo, mejores prácticas y el uso de Catalyst:

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Iniciar Spark
spark = SparkSession.bulder.appName("Optimization Ejemplo").getOrCreate()

# Leer datos
df1 = spark.read.csv("data.csv", header=True, inferSchema=True)
df2 = spark.read.csv("data2.csv", header=True, inferSchema=True)

# Aplicar mejores prácticas
df1 = df1.repartition(100) # Reparticionar
df2 = df2.cache() # Cache

# Unir DataFrames con Broadcast Join
dfjoined = df1.join(broadcast(df2), "id")

# Ver el plan de ejecución
df\_joined.explain()

df\_joined.show()
```

## En resumen

-   Monitoreo: Usa Spark UI, logs y métricas para supervisar el rendimiento.
-   Mejores prácticas: Evita operaciones costosas, usa esquemas explícitos, cachea DataFrames y particiona adecuadamente.
-   Catalyst Optimizer: Aprovecha el optimizador para mejorar el rendimiento de tus consultas.

## ***Fin del laboratorio***
