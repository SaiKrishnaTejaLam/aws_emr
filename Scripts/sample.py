from pyspark import SparkConf, SparkContext

#Initialize SparkContext

conf = SparkConf().setAppName("WordCount")

sc = SparkContext(conf=conf)

#Predefined input string

input_string= """

Spark is great for big data processing. Spark is fast and easy to use.

Spark supports various languages including Python, Scala, Java, and R.

"""
# Parallelize the predefined string to create an RDD

text_rdd = sc.parallelize(input_string.split("\\n"))

# Perform word count

word_counts = text_rdd.flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

# Save the output to S3 (replace with your output path)

word_counts.saveAsTextFile("s3://dataengineering-emr-serverless/output/word\_count")

sc.stop()