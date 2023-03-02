import sys

from pyspark.sql import SparkSession

from pyspark.sql.functions import count

if __name__ =="__main__":
    # if len(sys.argv) != 2:
    #     print("usage :mnmcount <file>", file=sys.stderr)
    #     sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("pythonMnMCount")
             .getOrCreate())

    mnmfile = "file:///D://sparkdatset//mnm_dataset.csv"

    mnm_df = (spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(mnmfile))
    count_mnm_df = (mnm_df.select("State","Color","count")
                    .groupBy("state","Color")
                    .agg(count("Count").alias("Total"))
                    .orderBy("Total",ascending=False))
    count_mnm_df.show(n=60,truncate=False)
    print("Total Rows = %d"%(count_mnm_df.count()))

    ca_count_mnm_df = (mnm_df.select("State","Color","Count")
                       .where(mnm_df.State == "CA")
                       .groupBy("State","Color")
                       .agg(count("Count").alias("Total"))
                       .orderBy("Total", ascending=False)
                       )

    spark.stop()