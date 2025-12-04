import os
import io
import tarfile
import urllib.request
import scipy.io.wavfile as wav
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, BooleanType

DATA_URL = "http://www.openslr.org/resources/1/waves_yesno.tar.gz"
DATA_DIR = "datos_audio"
TAR_FILE = "waves_yesno.tar.gz"

def descargar_datos():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)
        urllib.request.urlretrieve(DATA_URL, TAR_FILE)
        with tarfile.open(TAR_FILE, "r:gz") as tar:
            tar.extractall(path=DATA_DIR)
        os.remove(TAR_FILE)


# Estas funciones se ejecutan en PARALELO en cada nodo (worker) del cluster.
def extraer_duracion(contenido_binario):
    """
    Toma bytes crudos, los convierte a un objeto archivo en memoria,
    lee el WAV y calcula la duración en segundos.
    """
    try:
        with io.BytesIO(contenido_binario) as wav_file:
            sr, data = wav.read(wav_file)
            duracion = float(len(data) / sr)
            return duracion
    except Exception as e:
        return None

def main():
    descargar_datos()
    #Iniciar Spark
    spark = SparkSession.builder \
        .appName("AudioProcessingPipeline") \
        .master("local[*]") \
        .getOrCreate()

    #Cargar datos crudos
    ruta_audios = os.path.join(DATA_DIR, "waves_yesno")
    df_raw = spark.read.format("binaryFile") \
        .option("pathGlobFilter", "*.wav") \
        .option("recursiveFileLookup", "true") \
        .load(ruta_audios)

    #Registrar la UDF en Spark
    udf_duracion = udf(extraer_duracion, FloatType())

    #Transformación
    print("Procesando audios (Extracción de Features)...")
    
    df_procesado = df_raw.withColumn("duracion_sec", udf_duracion(col("content")))

    #  Limpieza y Filtrado
    df_clean = df_procesado.filter(col("duracion_sec").isNotNull()) \
                           .filter(col("duracion_sec") > 1.0) \
                           .select("path", "duracion_sec")


    df_clean.cache()
    
    cantidad = df_clean.count()
    promedio = df_clean.agg({"duracion_sec": "avg"}).collect()[0][0]

    print(f"Reporte del Pipeline:")
    print(f" Archivos procesados correctamente: {cantidad}")
    print(f" Duración promedio del audio: {promedio:.2f} segundos")

    df_clean.show(5, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()