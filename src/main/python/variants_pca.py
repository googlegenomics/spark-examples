# export SPARK_CLASSPATH=...
import json
import numpy
import operator
import sys

import pyspark
from pyspark import serializers
import pyspark.conf
from pyspark.mllib import common as mllib_common
from pyspark.mllib import linalg
import pyspark.rdd

conf = pyspark.conf.SparkConf()
sc = pyspark.SparkContext(conf=conf)

def prepare_call_data(py_rdd, py_id_to_index):
    """Return an RDD[Seq[int]] from the RDD[(VariantKey, Variant)].

    Args:
        py_rdd: An RDD of (VariantKey, Variant) of all Variants matching the
            search criteria.
        py_id_to_index: A dictionary of string to int, giving the indices of
            callset names in ``py_rdd``.

    Returns:
        An RDD[Seq[int]] in the same order of the input RDD, each entry is a
        list of indices of variant calls.
    """

    # Obtain all samples that have at least one matching call.
    samples_with_variant = (py_rdd.
        map(lambda (variant_key, variant): variant).
        map(lambda v: v.get('calls', [])).
        map(lambda calls: [c for c in calls if any(c['genotype'])]).
        filter(lambda calls: len(calls) > 0)
    )

    # Obtain the callset name from the samples.
    callset_names = (samples_with_variant.
        map(lambda callset: [c['callsetName'] for c in callset])
    )

    # Convert all names (strings) to indices (ints).
    sc = pyspark.SparkContext._active_spark_context
    broadcast_index_map = sc.broadcast(py_id_to_index)
    call_rdd = callset_names.map(
        lambda callset: [broadcast_index_map.value[c] for c in callset]
    )

    return call_rdd

def calculate_similarity_matrix(call_rdd, matrix_size):
    """Return an RDD[(int, int), int] where each entry is similarity value of
    call ``x``, with respect to call ``y``.

    Args:
        call_rdd: An RDD[Seq[int]] as returned by ``prepare_call_data``.
        matrix_size: The size (N) of the N x N matrix.

    Returns:
        An RDD[(x, y), sim_value] where each entry is similarity value of call
        ``x`` with respect to call ``y``.
    """

    def sum_similarity(callsets):
        matrix = numpy.zeros((matrix_size, matrix_size), numpy.int)
        for callset in callsets:
            for x in callset:
                for y in callset:
                    matrix[y][x] += 1
        for x in xrange(matrix_size):
            for y in xrange(matrix_size):
               yield (y, x), matrix[y][x]

    sim_matrix = (call_rdd.
        mapPartitions(sum_similarity).
        reduceByKey(operator.add)
    )

    return sim_matrix

def center_matrix(sim_matrix, row_count):
    """Center the rows and columns of a similarity matrix.

    Args:
        sim_matrix: A similarity matrix as returned by
            ``calculate_similarity_matrix``.
        row_count: The size (N) of the N x N matrix.

    Returns:
        An RDD[int, (int, float)] representing centered rows. The first int is
        the row index, the (int, float) tuple is the column index, and the
        centered value.
    """

    # Row-by-row (row major) RDD. Each row is a list of (column, value).
    entries = (sim_matrix.
        map(lambda ((y, x), v): (y, (x, float(v)))).
        groupByKey().
        sortByKey(True).
        cache()
    )
    row_sums = entries.map(lambda (y, xvs): sum(v for (x, v) in xvs)).collect()
    matrix_sum = sum(row_sums)
    matrix_mean = float(matrix_sum) / row_count / row_count

    sc = pyspark.SparkContext._active_spark_context
    broadcast_row_sums = sc.broadcast(row_sums)

    def center_rows((row, col_vals)):
        row_mean = broadcast_row_sums.value[row] / float(row_count)

        def center_cols(col, val):
            col_mean = broadcast_row_sums.value[col] / float(row_count)
            return (col, val - row_mean - col_mean + matrix_mean)

        return [center_cols(col, val) for col, val in col_vals]

    return entries.map(center_rows)

def perform_pca(matrix, row_count, nr_principal_components=2):
    """Return principal components of the input matrix.

    This function uses MLlib's ``RowMatrix`` to compute principal components.

    Args:
        matrix: An RDD[int, (int, float)] representing a sparse matrix. This
            is returned by ``center_matrix`` but it is not required to center
            the matrix first.
        row_count: The size (N) of the N x N ``matrix``.
        nr_principal_components: Number of components we want to obtain. This
            value must be less than or equal to the number of rows in the input
            square matrix.

    Returns:
        An array of ``nr_principal_components`` columns, and same number of rows
        as the input ``matrix``. This array is a ``numpy`` array.
    """

    py_rdd = matrix.map(lambda row: linalg.Vectors.sparse(row_count, row))
    sc = pyspark.SparkContext._active_spark_context
    java_rdd = mllib_common._py2java(sc, py_rdd)
    scala_rdd = java_rdd.rdd()
    sc = pyspark.SparkContext._active_spark_context
    row_matrix = (sc._jvm.org.apache.spark.mllib.linalg.distributed.
        RowMatrix(scala_rdd)
    )
    pca = row_matrix.computePrincipalComponents(nr_principal_components)
    pca = mllib_common._java2py(sc, pca)
    return pca.toArray()

def pca(argv):
    sc = pyspark.SparkContext._active_spark_context
    args = sc._jvm.java.util.ArrayList()
    for arg in argv:
        args.append(arg)
    args = sc._jvm.scala.collection.JavaConversions.asScalaBuffer(args)
    jsc = sc._jsc.sc()

    pca_conf = (sc._jvm.com.google.cloud.genomics.spark.examples.
        PcaConf(args))
    pca_driver = (sc._jvm.com.google.cloud.genomics.spark.examples.
        VariantsPcaDriver(pca_conf, jsc))

    # Map of sample ID to an index in the list of all sample IDs.
    # e.g. the list ['NA20818', 'NA20819', 'NA20826'] produces the map
    # {'NA20818': 0, 'NA20819': 1, 'NA20826', 2}
    java_id_to_index = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        pca_driver.indexes())
    py_id_to_index = {}
    for k in java_id_to_index:
        py_id_to_index[k] = java_id_to_index[k]
    # This is the reverse map of the previous one.
    index_to_id = dict((v, k) for (k, v) in py_id_to_index.iteritems())

    # Obtain an RDD of all Variants matching PcaConf.
    scala_rdd = pca_driver.getJsonData()
    java_rdd = scala_rdd.toJavaRDD()
    # Convert it to Python RDD.
    py_rdd = pyspark.rdd.RDD(java_rdd, sc, serializers.UTF8Deserializer())
    py_rdd = py_rdd.map(lambda line: json.loads(line))

    call_rdd = prepare_call_data(py_rdd, py_id_to_index)

    row_count = len(py_id_to_index)
    sim_matrix = calculate_similarity_matrix(call_rdd, row_count)

    # According to MLlib docs, we do NOT need to center inputs.
    centered_rows = center_matrix(sim_matrix, row_count)

    result = perform_pca(centered_rows, row_count,
                         pca_conf.numPc().get().get())

    for i, components in enumerate(result):
        print '%s\t%s' % (index_to_id[i], '\t'.join(str(c) for c in components))

    sc.stop()

pca(sys.argv[1:])
