pip install apache-airflow
pip install pyspark
pip freeze > requirements.txt

Starting Spark server on local machine:

spark-class org.apache.spark.deploy.master.Master
spark-class org.apache.spark.deploy.worker.Worker spark://192.168.29.63:7077