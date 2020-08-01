from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.ml import Pipeline, PipelineModel

def process(t, rdd):
	df = rdd.map(lambda r:Row(message=r))
	pipeline = PipelineModel.load("use_cases/spam_model4.4")
	predictions = pipeline.transform(df)
	predictions.select("message","prediction").write.format('jdbc').options(
		url='jdbc:mysql://mysqldb.edu.cloudlab.com/use_cases',
		driver='com.mysql.jdbc.Driver',
		dbtable='spam_message',
		user='labuser',
		password='edureka'
	).mode('append').save()
	
if __name__ == "__main__":
	sc = SparkContext(appName="PythonSparmStreaming")
	ssc = StreamingContext(sc, 10)
	lines = ssc.textFileStream("tmp/kafka/spam_message")
	lines.foreachRDD(process)
	ssc.start()
	ssc.awaitTermination()
	

