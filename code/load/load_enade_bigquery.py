from pyspark.sql import SparkSession


if __name__ == '__main__':

    temp_bucket = "gs://bootcamp-edc/temp"

    spark = (
        SparkSession
        .builder
        .appName('load_enad_bigquery')
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0")
        .config("spark.jars", "./jars/gcs-connector-hadoop3-latest.jar")
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("fs.gs.auth.service.account.enable", "true")
        .config("fs.gs.auth.service.account.json.keyfile", "/etc/gcp/sa_credentials.json")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    file_path = "gs://bootcamp-edc/trusted/microdados_enade/*.parquet"

    df_enade = spark.read.parquet(file_path)


    spark.conf.set("credentialsFile", "/etc/gcp/sa_credentials.json")
    
    (
        df_enade
        .write
        .format('bigquery')
        .option('table', 'bootedc.microdados_enade')
        .save()
    )
    
    del(df_enade)
    
    spark.stop()