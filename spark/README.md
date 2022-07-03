# PageRank in Apache Spark

## Compile

Execute the following command to compile the PageRank application: `mvn compile`.

## Create JAR

Make bundle with: `mvn package`.

## Submit to spark

```
[hadoop@Cygnus spark]$ spark-submit --master spark://25.72.178.57:7077 \
--class com.mycompany.app.PageRank target/spark-1.0-SNAPSHOT.jar \
$HOME/spark/data/mllib/pagerank_data.txt 2
```
