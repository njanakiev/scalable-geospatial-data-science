// spark-shell --master yarn -i gdelt_event_count.scala
// real    1m11.478s
// user    1m16.376s
// sys     0m3.083s

println("GDELT Event Code Count")

val parquetDF = spark.read.parquet("hdfs:///user/hadoop/gdelt_parquet/2020.snappy.parq")

parquetDF.createOrReplaceTempView("parquetFile")
val countsDF = spark.sql("""
    SELECT COUNT(*) AS event_count, event_root_code
    FROM parquetFile 
    GROUP BY event_root_code
    ORDER BY event_count DESC
""")

countsDF.show(5)

System.exit(0)
