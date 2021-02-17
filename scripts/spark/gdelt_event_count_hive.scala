// spark-shell --master yarn -i gdelt_event_count_hive.scala
// real    3m11.222s
// user    1m19.898s
// sys     0m4.304s

// System.setProperty("hive.metastore.uris", "thrift://node-master:9083");

println("GDELT Event Code Count")

import org.apache.spark.sql.hive.HiveContext

val hiveContext = new HiveContext(sc)
hiveContext.setConf("hive.metastore.uris", "thrift://node-master:9083")

val countsDF = hiveContext.sql("""
    SELECT COUNT(*) AS event_count, event_root_code
    FROM gdelt_csv 
    GROUP BY event_root_code
    ORDER BY event_count DESC
""")
                               
countsDF.show(5)

System.exit(0)
