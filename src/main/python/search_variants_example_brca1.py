# export SPARK_CLASSPATH=...
import pyspark
import pyspark.rdd
import pyspark.conf
import json

conf = pyspark.conf.SparkConf()
sc = pyspark.SparkContext(conf=conf)

def brca1():
    args = sc._gateway.new_array(sc._jvm.java.lang.String, 0)
    jsc = sc._jsc.sc()

    scala_rdd = (sc._jvm.com.google.cloud.genomics.spark.examples.
        SearchVariantsExampleBRCA1.createJsonRdd(jsc, args))

    INPUT_FILE = 'brca1'
    scala_rdd.saveAsTextFile(INPUT_FILE)
    rdd = sc.textFile(INPUT_FILE).map(lambda line: json.loads(line))

    print 'We have %d records that overlap BRCA1.' % rdd.count()
    print 'But only %d records are of a variant.' % rdd.filter(
        lambda (k, v): v['referenceBases'] != 'N').count()
    print 'The other %d records are reference-matching blocks.' % rdd.filter(
        lambda (k, v): v['referenceBases'] == 'N').count()

    sc.stop()

brca1()
