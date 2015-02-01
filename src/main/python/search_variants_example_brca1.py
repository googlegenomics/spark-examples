# export SPARK_CLASSPATH=...
import json
import pyspark
import pyspark.rdd
import pyspark.conf
from pyspark.serializers import UTF8Deserializer

conf = pyspark.conf.SparkConf()
sc = pyspark.SparkContext(conf=conf)

def brca1():
    args = sc._gateway.new_array(sc._jvm.java.lang.String, 0)
    jsc = sc._jsc.sc()

    scala_rdd = (sc._jvm.com.google.cloud.genomics.spark.examples.
        SearchVariantsExampleBRCA1.createJsonRdd(jsc, args))

    java_rdd = scala_rdd.toJavaRDD()

    py_rdd = pyspark.rdd.RDD(java_rdd, sc, UTF8Deserializer())
    py_rdd = py_rdd.map(lambda line: json.loads(line))
    py_rdd.cache()

    print 'We have %d records that overlap BRCA1.' % py_rdd.count()
    print 'But only %d records are of a variant.' % py_rdd.filter(
        lambda (k, v): v['referenceBases'] != 'N').count()
    print 'The other %d records are reference-matching blocks.' % py_rdd.filter(
        lambda (k, v): v['referenceBases'] == 'N').count()

    sc.stop()

brca1()
