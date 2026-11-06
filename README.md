# Dedup Backup

Dedup Backup es una herramienta para automatizar respaldos mediante deduplicación basada en contenido. Utiliza el algoritmo **BLAKE3** para identificar archivos de forma única y almacena la información en una base de datos **PostgreSQL**, evitando copiar archivos cuyo contenido ya existe.

El proyecto está diseñado para trabajar con grandes volúmenes de datos distribuidos en múltiples discos, priorizando el rendimiento mediante procesamiento paralelo y una arquitectura desacoplada entre exploración, hashing, consulta a la base de datos y copiado.

## Características

* Deduplicación basada en el hash **BLAKE3**.
* Base de datos **PostgreSQL** para el registro de hashes y metadatos.
* Copia únicamente de archivos cuyo contenido no existe previamente.
* Procesamiento paralelo para aprovechar procesadores multinúcleo.
* Ejecución simultánea de múltiples lectores, uno por cada disco o fuente de datos.
* Número configurable de *workers* para el cálculo de hashes.
* Procesamiento por lotes (*batches*) para reducir el número de consultas a la base de datos y mejorar el rendimiento.
* Sistema de perfiles para diferentes configuraciones de respaldo.
* Registro de eventos mediante archivos de log.
* Sistema de colas para desacoplar las distintas etapas del procesamiento.

## Arquitectura

El sistema está compuesto por un proceso principal y uno o varios de procesamiento, permitiendo automatizar completamente la ingesta y deduplicación de dispositivos de almacenamiento.

### 1. Proceso principal

El proceso principal permanece en ejecución esperando nuevas fuentes de datos. Estas pueden provenir de dos formas:

* Conexión automática de un dispositivo de almacenamiento, como memorias USB, discos duros externos, SSD externos o tarjetas SD.
* Ejecución manual desde el menú, seleccionando una carpeta o un disco específico.

Cuando se detecta un nuevo dispositivo, el proceso espera a que el sistema operativo complete su montaje. Una vez que el volumen está disponible y es accesible, inicia automáticamente el procesamiento correspondiente utilizando el perfil configurado, donde el perfil es una base de datos y un path donde se almacenarán los resultados.

### 2. Exploración del sistema de archivos

El proceso de exploración recorre recursivamente todos los directorios de la fuente de datos.

Para evitar trabajo innecesario, únicamente procesa archivos cuyas extensiones pertenecen a un conjunto inmutable (*frozen set*) de extensiones permitidas. Este mecanismo permite realizar búsquedas muy rápidas y filtrar inmediatamente archivos que no forman parte del respaldo.

Cada archivo válido es enviado al proceso de hashing.

### 3. Hashing paralelo

El cálculo de hashes se realiza utilizando un conjunto configurable de *workers*.

Cada *worker* procesa archivos de forma independiente calculando su hash mediante el algoritmo **BLAKE3**, aprovechando múltiples núcleos del procesador para maximizar el rendimiento.

El número de *workers* puede configurarse para cada lector según el hardware disponible.

### 4. Procesamiento por lotes

Los hashes calculados no se consultan individualmente.

Se agrupan en lotes (*batches*) de tamaño configurable y posteriormente se realizan consultas masivas sobre PostgreSQL para determinar cuáles archivos ya existen dentro de la base de datos.

Este enfoque reduce considerablemente el número de consultas y mejora el rendimiento cuando se procesan cientos de miles o millones de archivos.

### 5. Copiado y registro

Para cada lote procesado:

* Los archivos cuyo hash ya existe en PostgreSQL son descartados.
* Los archivos nuevos son asignados como pendientes durante el copiado al directorio definido por el perfil activo.
* Una vez copiados correctamente, se registran como finalizados en PostgreSQL junto con su hash BLAKE3 y sus metadatos.

De esta forma se garantiza que un mismo contenido nunca sea almacenado dos veces, independientemente de su nombre o ubicación original.

## Paralelismo

La arquitectura permite ejecutar múltiples lectores de manera simultánea.

Cada lector puede trabajar sobre una fuente distinta (por ejemplo, un disco externo diferente) y configurarse de manera independiente con:

* Número de *workers* para el cálculo de hashes.
* Tamaño de los lotes (*batch size*).
* Directorio de destino.
* Perfil de respaldo.
* Base de datos PostgreSQL.

Esto permite aprovechar varios dispositivos de almacenamiento al mismo tiempo, distribuyendo la carga entre CPU, memoria y discos para maximizar el rendimiento del proceso de deduplicación.

## Paralelismo

El programa fue diseñado para escalar con el hardware disponible.

Es posible ejecutar múltiples instancias del lector, asignando una a cada disco físico. Cada instancia puede configurarse de manera independiente con:

* Número de *workers* dedicados al cálculo de hashes.
* Tamaño de los lotes (*batch size*).
* Base de datos de destino.
* Directorio de respaldo.

De esta forma es posible aprovechar mejor el ancho de banda de múltiples discos, distribuir la carga entre diferentes dispositivos de almacenamiento y ajustar el consumo de CPU según el equipo.

## Requisitos

* Linux
* Python 3.11 o superior
* PostgreSQL
* Entorno virtual de Python (recomendado)

## Instalación

```bash
git clone <URL_DEL_REPOSITORIO>
cd dedup-backup

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

Configura PostgreSQL y crea la base de datos que almacenará los hashes y metadatos de los archivos.

## Uso

### Iniciar un lector automatizado sobre un unico perfil, plug and play para comenzar un backup

```bash
/home/uzr/v_envs/Scrapper/bin/python3 ./listener.py \
    --db-dsn "dbname=scrapper" \
    --output-root /home/uzr/data_full
```

Pueden ejecutarse varias instancias del lector simultáneamente para distintos discos.

### Ejecutar el menú principal

```bash
/home/uzr/v_envs/Scrapper/bin/python ./menu.py
```

Desde el menú es posible:

* Seleccionar un perfil de respaldo.
* Elegir la base de datos.
* Seleccionar el directorio de destino.
* Escanear una carpeta.
* Escanear un disco completo.
* Iniciar el proceso de deduplicación y copia.

## Funcionamiento

Para cada archivo:

1. Se localiza el archivo durante la exploración.
2. Se calcula su hash BLAKE3.
3. El hash se agrupa junto con otros en un lote.
4. PostgreSQL determina qué hashes ya existen.
5. Solo los archivos nuevos se copian al destino.
6. Finalmente se registran el hash, la ruta y los metadatos correspondientes.

Este enfoque evita realizar consultas individuales por archivo y reduce significativamente el tiempo total de procesamiento cuando se manejan cientos de miles o millones de archivos.

## Estado del proyecto

El proyecto se encuentra en desarrollo activo. Entre las mejoras planeadas se incluyen:

* Interfaz TUI con Textual.
* Instalador automático.
* Archivo de configuración centralizado.
* Empaquetado como aplicación instalable.
* API para automatización.
* Métricas de rendimiento y monitoreo.
* Pruebas automatizadas.
* Hashing por ssh con calculo local para solo enviar hashes y hacer rsync
## Licencia

Pendiente de definir.

# Casos de uso
Si tienes un montón de carpetas de distintas fuentes y no quieres filtrarlas manualmente.
Si tienes que crear un backup de archivos para comenzando desde cero.