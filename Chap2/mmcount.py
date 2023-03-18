# 예제 2-1 M&M 개수 집계

import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())

    mnm_file = sys.argv[1]

    # csv 파일 읽어오기
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))

    # 주/색깔 별로 집계
    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupby("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    # show() 액션
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # 캘리포니아 주에 대해서만 집계
    ca_count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .where(mnm_df.State == "CA")
                    .groupby("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

    ca_count_mnm_df.show(n=10, truncate=False)

    # SparkSession stop
    spark.stop()