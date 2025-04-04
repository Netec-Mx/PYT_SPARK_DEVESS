# Práctica 1. Instalación de ambiente (Spark, Python y bibliotecas)

**Objetivo de la práctica:**
Al finalizar la práctica serás capaz de:
- Configurar el servidor de PySpark en Linux y el IDE PyCharm.

**Duración aproximada:**
- 60 minutos.

**Prerequisitos:**

- Acceso al ambiente Linux (credenciales provistas en el curso) o Linux local con interfaz gráfica.
- Conexión a internet.

**Instrucciones:**

## Tarea 1: Instalar prerrequisitos

**Iniciar la conexión al ambiente virtual**

Iniciar la conexión al servidor a través de RDP de acuerdo a la información provista en el curso:

<img src="./media/image1.png" style="width:4.38542in;height:2.77083in" />

Proveer las credenciales del usuario y contraseña provistos en el curso:

<img src="./media/image2.png" style="width:2.51182in;height:3.10364in" />

## Instalación de requisitos

Abrir una ventana de terminal. Desde aquí instalaremos el JDK de Java.

> **sudo apt-get install openjdk-8-jdk**

<img src="./media/image3.png" style="width:6.1375in;height:3.17639in" />

**Verificar la instalación**

Editar el archivo .bashrc para agregar los binarios de Java a la ruta del ambiente.

> **nano ~/.bashrc**
>
> **export JAVA\_HOME=/usr/lib/jvm/java-8-openjdk-amd64/**

<img src="./media/image4.png" style="width:4.21755in;height:2.79213in" />

Aplicar los cambios y verificar la versión de Java.

source ~/.bashrc

java –version

<img src="./media/image5.png" style="width:6.1375in;height:1.23264in" />

**Instalación PIP3, el manejador de paquetes de Python**

PIP es un gestor de paquetes y librerías para Python, permite gestionar dependencias de una manera muy sencilla. Primero, se actualizan los paquetes y se instala pip3:

sudo apt-get update

sudo apt-get install python3-pip

pip3 –version

<img src="./media/image6.png" style="width:6.1375in;height:2.22569in" />

Renombrar comando pip3 a pip (esto es opcional).

echo alias pip=pip3 &gt;&gt; ~/.bashrc

source ~/.bashrc

pip --version

<img src="./media/image7.png" style="width:6.1375in;height:1.01736in" />

## Tarea 2: Instalación de Spark

Ya con el prerrequisito de Java, ahora se instalará el motor de Spark. Para ello, hay que descargar el paquete desde la página de Apache Spark. Se descarga el paquete de acuerdo a la versión deseada.

<img src="./media/image8.png" style="width:4.71331in;height:2.54758in" />

<img src="./media/image9.png" style="width:5.85853in;height:1.98797in" />

Descomprimir el paquete con el siguiente comando:

tar -xvzf

<img src="./media/image10.png" style="width:3.80208in;height:2.49913in" />

No es estrictamente necesario, pero se puede mover el directorio de Spark a una ubicación más accesible:

sudo mv spark-3.5.4-bin-hadoop3 /usr/local/spark

## 

## Tarea 3: Configuración de variables

Establecer las variables necesarias para Spark, de acuerdo al directorio donde se haya desempaquetado. Para ello, con el editor preferido, editar el archivo .bashrc.

nano ~/.bashrc

export SPARK\_HOME=/usr/local/spark

export PATH=$PATH:$SPARK\_HOME/bin

export PYTHONPATH=$SPARK\_HOME/python:$SPARK\_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

export PYSPARK\_PYTHON=python3

>***Importante: El nombre del archivo py4j dependerá de la versión que se tenga instalada***

<img src="./media/image11.png" style="width:6.1375in;height:3.15486in" />

Guardar los cambios y salir del editor. 
Aplicar estos cambios con el siguiente comando:

source: ~/.bashrc

**Probar al acceso a pyspark**

Una vez instalado, hacemos la prueba desde línea a terminal:

pyspark

<img src="./media/image12.png" style="width:5.92021in;height:1.91667in" />

> ***Nota: En ocasiones, puede aparecer un error indicando que no puede acceder al servidor. Si esto ocurre, editar el archivo load-spark-env.sh en el directorio spark/bin y adicionar la siguiente línea:***

***export SPARK\_LOCAL\_IP="127.0.0.1"***

Nótese que se establecieron diferentes elementos por omisión:

Spark context Web UI available at http://pyspark1.internal.cloudapp.net:4040

Spark context available as 'sc' (master = local\[\*\], app id = local-1737635834756).

SparkSession available as 'spark'.

Probar la instalación con los siguientes comandos:

print(sc)

print(sc.version)

print(spark)

exit()

<img src="./media/image13.png" style="width:4.79143in;height:2.4933in" />

## Tarea 4: Instalación de PyCharm

Vamos a requerir una herramienta para trabajar con el código de PySpark. Existen varias opciones, pero **PyCharm** es una de las mejores IDEs. 
- Tiene versiones de la comunidad y empresarial.
- Cuenta con diferentes características para hacer muy productivo el desarrollo.

Actualizar Linux con los siguientes comandos:

sudo apt-get update -y

sudo apt-get upgrade -y

<img src="./media/image14.png" style="width:4.53873in;height:2.03467in" />

Para instalar PyCharm en el sistema, debes tener instalada la utilidad Snap. Ejecutar el comando para instalarla.

sudo apt install snapd

Hay varias formas de instalar las diferentes ediciones de PyCharm. El siguiente comando instalará la edición de la comunidad desde la línea de comandos:

sudo snap install pycharm-community --classic

<img src="./media/image15.png" style="width:4.03892in;height:2.66428in" />

Iniciar con

pycharm-community

<img src="./media/image16.png" style="width:4.19043in;height:2.76042in" />

Aceptar el acuerdo de licencia. Opcionalmente podemos aceptar o negar el compartir información de sesiones.

<img src="./media/image17.png" style="width:3.9837in;height:3.03353in" />

<img src="./media/image18.png" style="width:3.62375in;height:2.75369in" />

Finalmente, tenemos la pantalla de inicio.

<img src="./media/image19.png" style="width:4.32608in;height:3.50374in" />

**Fin del laboratorio**
