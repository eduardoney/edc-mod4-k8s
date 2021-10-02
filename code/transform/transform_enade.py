# import libraries
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as f

# set default location for warehouse
warehouse_location = abspath('spark-warehouse')

if __name__ == '__main__':

    spark = (SparkSession
             .builder
             .appName('transform_enade')
             .config("spark.sql.warehouse.dir", warehouse_location)
             .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-latest.jar")
             #.config("spark.jars.packages","com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2")
             .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
             .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
             .config("fs.gs.auth.service.account.enable", "true")
             .config("fs.gs.auth.service.account.json.keyfile", "/etc/gcp/sa_credentials.json")
             .config("spark.driver.memory", "8g")
             .enableHiveSupport()
             .getOrCreate())

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("warn")

    file_path = 'gs://bootcamp-edc/landing/MICRODADOS_ENADE_2017.txt'

    print('Inicio leitura arquivo')
    
    df_enade = (spark
                .read
                .format('csv')
                .option("sep", ";")
                .option("header", "true")
                .option("inferSchema", "false")
                .csv(file_path))

    int_columns = ['NU_ANO', 'CO_IES', 'CO_CATEGAD', 'CO_ORGACAD', 'CO_GRUPO', 'CO_CURSO', 'CO_MODALIDADE', 'CO_MUNIC_CURSO', 'CO_UF_CURSO',
                   'CO_REGIAO_CURSO', 'NU_IDADE', 'ANO_FIM_EM', 'ANO_IN_GRAD', 'CO_TURNO_GRADUACAO', 'TP_INSCRICAO_ADM', 'TP_INSCRICAO',
                   'NU_ITEM_OFG', 'NU_ITEM_OFG_Z', 'NU_ITEM_OFG_X', 'NU_ITEM_OFG_N', 'NU_ITEM_OCE', 'NU_ITEM_OCE_Z', 'NU_ITEM_OCE_X',
                   'NU_ITEM_OCE_N', 'TP_PRES', 'TP_PR_GER', 'TP_PR_OB_FG', 'TP_PR_DI_FG', 'TP_PR_OB_CE', 'TP_PR_DI_CE', 'TP_SFG_D1',
                   'TP_SFG_D2', 'TP_SCE_D1', 'TP_SCE_D2', 'TP_SCE_D3', 'QE_I16', 'QE_I27', 'QE_I28', 'QE_I29', 'QE_I30', 'QE_I31', 'QE_I32',
                   'QE_I33', 'QE_I34', 'QE_I35', 'QE_I36', 'QE_I37', 'QE_I38', 'QE_I39', 'QE_I40', 'QE_I41', 'QE_I42', 'QE_I43',
                   'QE_I44', 'QE_I45', 'QE_I46', 'QE_I47', 'QE_I48', 'QE_I49', 'QE_I50', 'QE_I51', 'QE_I52', 'QE_I53', 'QE_I54',
                   'QE_I55', 'QE_I56', 'QE_I57', 'QE_I58', 'QE_I59', 'QE_I60', 'QE_I61', 'QE_I62', 'QE_I63', 'QE_I64', 'QE_I65',
                   'QE_I66', 'QE_I67', 'QE_I68']

    for column in int_columns:
        df_enade = df_enade.withColumn(column, f.col(column).cast('int'))

    float_columns = ['NT_GER', 'NT_FG', 'NT_OBJ_FG', 'NT_DIS_FG', 'NT_FG_D1',
                     'NT_FG_D1_PT', 'NT_FG_D1_CT', 'NT_FG_D2', 'NT_FG_D2_PT', 'NT_FG_D2_CT', 'NT_CE', 'NT_OBJ_CE', 'NT_DIS_CE',
                     'NT_CE_D1', 'NT_CE_D2', 'NT_CE_D3']

    for column in float_columns:
        df_enade = df_enade.withColumn(column, f.regexp_replace(f.col(column),',','.').cast('float'))

    df_enade.printSchema()

    (
        df_enade
        .write
        .mode('overwrite')
        .parquet("gs://bootcamp-edc/trusted/microdados_enade")
    )

    print('Arquivo escrito com sucesso')

    del(df_enade)
    spark.stop()
