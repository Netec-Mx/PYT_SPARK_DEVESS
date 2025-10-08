# Práctica 1. Instalación de ambiente (Spark, Python y bibliotecas)

## **Objetivo**

Al finalizar la práctica, serás capaz de:
- Configurar el servidor de PySpark en Linux y el IDE PyCharm.

## **Duración aproximada**
- 60 minutos.

## **Prerrequisitos**

- Acceso al ambiente Linux (credenciales provistas en el curso) o Linux local con interfaz gráfica.
- Conexión a internet.

## Instrucciones

### Tarea 1. Instalar prerrequisitos: iniciar la conexión al ambiente virtual

**Paso 1.** Inicia la conexión al servidor a través de RDP, de acuerdo con la información provista en el curso.

<img src="./media/image1.png" style="width:4.38542in;height:2.77083in" />

- Provee las credenciales del usuario y contraseña provistos en el curso.

<img src="./media/image2.png" style="width:2.51182in;height:3.10364in" />

**Paso 2.** Instala los requisitos.

- Abre una ventana de terminal. Desde aquí, instala el JDK de Java.

```
sudo apt-get install openjdk-8-jdk
```

<img src="./media/image3.png" style="width:6.1375in;height:3.17639in" />

- Verifica la instalación.

- Edita el archivo `.bashrc` para agregar los binarios de Java a la ruta del ambiente.

```
nano ~/.bashrc

export JAVA\_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
```

<img src="./media/image4.png" style="width:4.21755in;height:2.79213in" />

- Aplica los cambios y verifica la versión de Java.

```
source ~/.bashrc

java –version

```

<img src="./media/image5.png" style="width:6.1375in;height:1.23264in" />

**Paso 3.** Instala PIP3, el manejador de paquetes de Python.

- PIP es un gestor de paquetes y librerías para Python, permite gestionar dependencias de una manera muy sencilla. Primero, actualiza los paquetes e instala pip3.

```
sudo apt-get update

sudo apt-get install python3-pip

pip3 –version

```

<img src="./media/image6.png" style="width:6.1375in;height:2.22569in" />

- Renombra el comando pip3 a pip (esto es opcional).

```
echo alias pip=pip3 &gt;&gt; ~/.bashrc

source ~/.bashrc

pip --version
```

<img src="./media/image7.png" style="width:6.1375in;height:1.01736in" />

### Tarea 2. Instalación de Spark

- Ya con el prerrequisito de Java, ahora instala el motor de Spark. Para ello, descarga el paquete desde la página de Apache Spark, de acuerdo con la versión deseada.

<img src="./media/image8.png" style="width:4.71331in;height:2.54758in" />

<img src="./media/image9.png" style="width:5.85853in;height:1.98797in" />

- Descomprime el paquete con el siguiente comando.

```
tar -xvzf
```

<img src="./media/image10.png" style="width:3.80208in;height:2.49913in" />

No es estrictamente necesario, pero se puede mover el directorio de Spark a una ubicación más accesible.

```
sudo mv spark-3.5.4-bin-hadoop3 /usr/local/spark
```


## Tarea 3. Configuración de variables

Establece las variables necesarias para Spark, de acuerdo con el directorio donde se haya desempaquetado. Para ello, con el editor preferido, edita el archivo `.bashrc`.


```
nano ~/.bashrc

export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin

export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=python3
```

>***Importante. El nombre del archivo py4j dependerá de la versión instalada.***

<img src="./media/image11.png" style="width:6.1375in;height:3.15486in" />

- Guarda los cambios y sal del editor. 
- Aplica estos cambios con el siguiente comando.

```
source: ~/.bashrc
```

- Prueba al acceso a pyspark.

Una vez instalado, realiza la prueba desde la línea a la terminal.

`pyspark`

<img src="./media/image12.png" style="width:5.92021in;height:1.91667in" />

> ***Nota. En ocasiones, puede aparecer un error indicando que no se puede acceder al servidor. Si esto ocurre, edita el archivo `load-spark-env.sh` en el directorio `spark/bin` y adiciona la siguiente línea: `export SPARK\_LOCAL\_IP="127.0.0.1"`.***


- Nota que se establecieron diferentes elementos por omisión.
```
- Spark context Web UI available at http://pyspark1.internal.cloudapp.net:4040.
- Spark context available as 'sc' (master = local\[\*\], app id = local-1737635834756).
- SparkSession available as 'spark'.
```
- Prueba la instalación con los siguientes comandos.

```
print(sc)

print(sc.version)

print(spark)

exit()
```

<img src="./media/image13.png" style="width:4.79143in;height:2.4933in" />

### Tarea 4. Instalación de PyCharm

Vas a requerir una herramienta para trabajar con el código de PySpark. Existen varias opciones, pero **PyCharm** es una de las mejores IDE. 
- Tiene versiones de la comunidad y para empresarial.
- Cuenta con diferentes características para hacer muy productivo el desarrollo.

- Actualiza Linux con los siguientes comandos.

```
sudo apt-get update -y

sudo apt-get upgrade -y
```

<img src="./media/image14.png" style="width:4.53873in;height:2.03467in" />

- Para instalar PyCharm en el sistema, debes instalar la utilidad Snap. Ejecuta el siguiente comando para instalarla.

```
sudo apt install snapd
```

- Hay varias formas de instalar las diferentes ediciones de PyCharm. El siguiente comando instalará la edición de la comunidad desde la línea de comandos.

```
sudo snap install pycharm-community --classic
```

<img src="./media/image15.png" style="width:4.03892in;height:2.66428in" />

- Inicia con:

```
pycharm-community
```

<img src="./media/image16.png" style="width:4.19043in;height:2.76042in" />

- Acepta el acuerdo de licencia. Opcionalmente, acepta o niega el compartir la información de las sesiones.

<img src="./media/image17.png" style="width:3.9837in;height:3.03353in" />

<img src="./media/image18.png" style="width:3.62375in;height:2.75369in" />

## Resultado esperado
Finalmente, tienes la pantalla de inicio.

<img src="./media/image19.png" style="width:4.32608in;height:3.50374in" />


