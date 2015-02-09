/home/liang/spark/bin/spark-submit --jars $(echo /home/liang/ScalaProjects/SparkSkyline/lib/*.jar | tr ' ' ',') \
--class "spark.SparkApp" --master local[4] /home/liang/ScalaProjects/SparkSkyline/target/scala-2.10/spark-probskyline-project_2.10-1.0.jar
