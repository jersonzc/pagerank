# PageRank in Apache Spark

## Compile

Execute the following command to compile the PageRank application: `mvn compile`.

## Create JAR

Make bundle with: `mvn package`.

## Submit to spark

```
[hadoop@Cygnus spark]$ spark-submit --master spark://<ip_master>:7077 \
--class com.mycompany.app.PageRank target/spark-1.0-SNAPSHOT.jar \
$HOME/spark/data/mllib/pagerank_data.txt 2
```

## Results
```
4 has rank: 0.9149999999999999.
2 has rank: 0.9149999999999999.
3 has rank: 0.9149999999999999.
1 has rank: 1.255.
```
