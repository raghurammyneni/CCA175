hdfs dfs -mkdir /user/cloudera/adult
hdfs dfs -put Desktop/adult.data /user/cloudera/adult

val adultRDD = sc.textFile("/user/cloudera/adult/adult.data")
adultRDD: org.apache.spark.rdd.RDD[String] = /user/cloudera/adult/adult.data MapPartitionsRDD[1] at textFile at <console>:27

adultRDD.take(5)
res0: Array[String] = Array(39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K, 50, Self-emp-not-inc, 83311, Bachelors, 13, Married-civ-spouse, Exec-managerial, Husband, White, Male, 0, 0, 13, United-States, <=50K, 38, Private, 215646, HS-grad, 9, Divorced, Handlers-cleaners, Not-in-family, White, Male, 0, 0, 40, United-States, <=50K, 53, Private, 234721, 11th, 7, Married-civ-spouse, Handlers-cleaners, Husband, Black, Male, 0, 0, 40, United-States, <=50K, 28, Private, 338409, Bachelors, 13, Married-civ-spouse, Prof-specialty, Wife, Black, Female, 0, 0, 40, Cuba, <=50K)


adultRDD.count
res6: Long = 32562


adultRDD.map(line => line.split(",").length).distinct.collect
res3: Array[Int] = Array(15, 1)

val filteredRDD = adultRDD.filter(line => line.split(",").length == 15)
filteredRDD: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[7] at filter at <console>:29

filteredRDD.count
res7: Long = 32561


import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val adultSchema = StructType(Array(StructField("age", IntegerType), StructField("workclass", StringType), StructField("fnlwgt", IntegerType), StructField("education", StringType), StructField("education-num", IntegerType), StructField("marital-status", StringType), StructField("occupation", StringType), StructField("relationship", StringType), StructField("race", StringType), StructField("sex", StringType), StructField("capital-gain", IntegerType), StructField("capital-loss", IntegerType), StructField("hours-per-week", IntegerType), StructField("native-country", StringType), StructField("income", StringType)))

filteredRDD.take(1)
res8: Array[String] = Array(39, State-gov, 77516, Bachelors, 13, Never-married, Adm-clerical, Not-in-family, White, Male, 2174, 0, 40, United-States, <=50K)

val formattedRDD = filteredRDD.map(line => line.replace(" ", "").split(",")).map(x => Row(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toInt, x(5), x(6), x(7), x(8), x(9), x(10).toInt, x(11).toInt, x(12).toInt, x(13), x(14)))
formattedRDD: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = MapPartitionsRDD[20] at map at <console>:38

formattedRDD.take(1)
res12: Array[org.apache.spark.sql.Row] = Array([39,State-gov,77516,Bachelors,13,Never-married,Adm-clerical,Not-in-family,White,Male,2174,0,40,United-States,<=50K])


val adultDF = sqlContext.createDataFrame(formattedRDD, adultSchema)
adultDF: org.apache.spark.sql.DataFrame = [age: int, workclass: string, fnlwgt: int, education: string, education-num: int, marital-status: string, occupation: string, relationship: string, race: string, sex: string, capital-gain: int, capital-loss: int, hours-per-week: int, native-country: string, income: string]

adultDF.show(5)

adultDF.printSchema
root
 |-- age: integer (nullable = true)
 |-- workclass: string (nullable = true)
 |-- fnlwgt: integer (nullable = true)
 |-- education: string (nullable = true)
 |-- education-num: integer (nullable = true)
 |-- marital-status: string (nullable = true)
 |-- occupation: string (nullable = true)
 |-- priv-house-serv: string (nullable = true)
 |-- relationship: string (nullable = true)
 |-- race: string (nullable = true)
 |-- sex: string (nullable = true)
 |-- capital-gain: integer (nullable = true)
 |-- capital-loss: integer (nullable = true)
 |-- hours-per-week: integer (nullable = true)
 |-- native-country: string (nullable = true)



//number of records per work calss
adultDF.groupBy("workclass").count().foreach(println)
[Private,22696]
[Federal-gov,960]
[State-gov,1298]
[?,1836]
[Local-gov,2093]
[Self-emp-inc,1116]
[Without-pay,14]
[Never-worked,7]
[Self-emp-not-inc,2541]

//number of records per sec
adultDF.groupBy("sex").count().foreach(println)
[Female,10771]
[Male,21790]

//number of records by country
adultDF.groupBy(adultDF("native-country")).count().orderBy("native-country").show(50)
+--------------------+-----+
|      native-country|count|
+--------------------+-----+
|                   ?|  583|
|            Cambodia|   19|
|              Canada|  121|
|               China|   75|
|            Columbia|   59|
|                Cuba|   95|
|  Dominican-Republic|   70|
|             Ecuador|   28|
|         El-Salvador|  106|
|             England|   90|
|              France|   29|
|             Germany|  137|
|              Greece|   29|
|           Guatemala|   64|
|               Haiti|   44|
|  Holand-Netherlands|    1|
|            Honduras|   13|
|                Hong|   20|
|             Hungary|   13|
|               India|  100|
|                Iran|   43|
|             Ireland|   24|
|               Italy|   73|
|             Jamaica|   81|
|               Japan|   62|
|                Laos|   18|
|              Mexico|  643|
|           Nicaragua|   34|
|Outlying-US(Guam-...|   14|
|                Peru|   31|
|         Philippines|  198|
|              Poland|   60|
|            Portugal|   37|
|         Puerto-Rico|  114|
|            Scotland|   12|
|               South|   80|
|              Taiwan|   51|
|            Thailand|   18|
|     Trinadad&Tobago|   19|
|       United-States|29170|
|             Vietnam|   67|
|          Yugoslavia|   16|
+--------------------+-----+


// % of records by country 
adultDF.groupBy(col("native-country")).agg(((count(col("native-country"))/adultDF.count)*100).alias("percentage")).show(50)
+--------------------+--------------------+                                     
|      native-country|          percentage|
+--------------------+--------------------+
|            Thailand|0.055280857467522496|
|              Mexico|  1.9747550750898315|
|           Guatemala|  0.1965541598845244|
|               India|  0.3071158748195694|
|            Portugal|  0.1136328736832407|
|         Puerto-Rico| 0.35011209729430914|
|              Poland| 0.18426952489174164|
|                Hong| 0.06142317496391389|
|              Canada|   0.371610208531679|
|           Nicaragua|  0.1044193974386536|
|  Dominican-Republic|  0.2149811123736986|
|  Holand-Netherlands|0.003071158748195694|
|                Cuba| 0.29176008107859097|
|                   ?|  1.7904855501980899|
|             Ireland| 0.07370780995669667|
|             England| 0.27640428733761246|
|                Peru| 0.09520592119406653|
|            Scotland| 0.03685390497834833|
|               South| 0.24569269985565556|
|               China| 0.23033690611467708|
|                Iran| 0.13205982617241485|
|             Vietnam|  0.2057676361291115|
|              Taiwan|  0.1566290961579804|
|             Hungary| 0.03992506372654402|
|              France| 0.08906360369767513|
|         Philippines|  0.6080894321427475|
|             Germany| 0.42074874850281013|
|             Ecuador| 0.08599244494947943|
|          Yugoslavia|  0.0491385399711311|
|               Japan| 0.19041184238813305|
|     Trinadad&Tobago| 0.05835201621571819|
|            Columbia| 0.18119836614354595|
|            Honduras| 0.03992506372654402|
|               Haiti| 0.13513098492061054|
|            Cambodia| 0.05835201621571819|
|                Laos|0.055280857467522496|
|         El-Salvador| 0.32554282730874357|
|       United-States|    89.5857006848684|
|Outlying-US(Guam-...| 0.04299622247473971|
|              Greece| 0.08906360369767513|
|               Italy| 0.22419458861828567|
|             Jamaica| 0.24876385860385122|
+--------------------+--------------------+


// % of records by sex
adultDF.groupBy(col("sex")).agg(((count(col("sex"))/adultDF.count)*100).alias("percentage")).show(50)
+------+-----------------+                                                      
|   sex|       percentage|
+------+-----------------+
|Female|33.07945087681583|
|  Male|66.92054912318419|
+------+-----------------+

//Avg hours per week
adultDF.groupBy(col("sex")).agg(((sum(col("hours-per-week")))/count(col("sex"))).alias("avg-hours-per-week")).show()
+------+------------------+                                                     
|   sex|avg-hours-per-week|
+------+------------------+
|Female|36.410361154953115|
|  Male| 42.42808627810923|
+------+------------------+

// All education categories
(adultDF.select(col("education").alias("all-edu-cat")).distinct).orderBy(col("all-edu-cat")).show
+------------+                                                                  
| all-edu-cat|
+------------+
|        10th|
|        11th|
|        12th|
|     1st-4th|
|     5th-6th|
|     7th-8th|
|         9th|
|  Assoc-acdm|
|   Assoc-voc|
|   Bachelors|
|   Doctorate|
|     HS-grad|
|     Masters|
|   Preschool|
| Prof-school|
|Some-college|
+------------+

//education by race
adultDF.groupBy(col("education"), col("race")).agg(count(col("race"))).orderBy(col("education"), col("race")).show(100)
+------------+------------------+-----------+                                   
|   education|              race|count(race)|
+------------+------------------+-----------+
|        10th|Amer-Indian-Eskimo|         16|
|        10th|Asian-Pac-Islander|         13|
|        10th|             Black|        133|
|        10th|             Other|          9|
|        10th|             White|        762|
|        11th|Amer-Indian-Eskimo|         14|
|        11th|Asian-Pac-Islander|         21|
|        11th|             Black|        153|
|        11th|             Other|         10|
|        11th|             White|        977|
|        12th|Amer-Indian-Eskimo|          5|
|        12th|Asian-Pac-Islander|          9|
|        12th|             Black|         70|
|        12th|             Other|         14|
|        12th|             White|        335|
|     1st-4th|Amer-Indian-Eskimo|          4|
|     1st-4th|Asian-Pac-Islander|          5|
|     1st-4th|             Black|         16|
|     1st-4th|             Other|          9|
|     1st-4th|             White|        134|
|     5th-6th|Amer-Indian-Eskimo|          2|
|     5th-6th|Asian-Pac-Islander|         18|
|     5th-6th|             Black|         21|
|     5th-6th|             Other|         13|
|     5th-6th|             White|        279|
|     7th-8th|Amer-Indian-Eskimo|          9|
|     7th-8th|Asian-Pac-Islander|         11|
|     7th-8th|             Black|         56|
|     7th-8th|             Other|         17|
|     7th-8th|             White|        553|
|         9th|Amer-Indian-Eskimo|          5|
|         9th|Asian-Pac-Islander|          9|
|         9th|             Black|         89|
|         9th|             Other|          8|
|         9th|             White|        403|
|  Assoc-acdm|Amer-Indian-Eskimo|          8|
|  Assoc-acdm|Asian-Pac-Islander|         29|
|  Assoc-acdm|             Black|        107|
|  Assoc-acdm|             Other|          8|
|  Assoc-acdm|             White|        915|
|   Assoc-voc|Amer-Indian-Eskimo|         19|
|   Assoc-voc|Asian-Pac-Islander|         38|
|   Assoc-voc|             Black|        112|
|   Assoc-voc|             Other|          6|
|   Assoc-voc|             White|       1207|
|   Bachelors|Amer-Indian-Eskimo|         21|
|   Bachelors|Asian-Pac-Islander|        289|
|   Bachelors|             Black|        330|
|   Bachelors|             Other|         33|
|   Bachelors|             White|       4682|
|   Doctorate|Amer-Indian-Eskimo|          3|
|   Doctorate|Asian-Pac-Islander|         28|
|   Doctorate|             Black|         11|
|   Doctorate|             Other|          2|
|   Doctorate|             White|        369|
|     HS-grad|Amer-Indian-Eskimo|        119|
|     HS-grad|Asian-Pac-Islander|        226|
|     HS-grad|             Black|       1174|
|     HS-grad|             Other|         78|
|     HS-grad|             White|       8904|
|     Masters|Amer-Indian-Eskimo|          5|
|     Masters|Asian-Pac-Islander|         88|
|     Masters|             Black|         86|
|     Masters|             Other|          7|
|     Masters|             White|       1537|
|   Preschool|Asian-Pac-Islander|          6|
|   Preschool|             Black|          5|
|   Preschool|             Other|          2|
|   Preschool|             White|         38|
| Prof-school|Amer-Indian-Eskimo|          2|
| Prof-school|Asian-Pac-Islander|         41|
| Prof-school|             Black|         15|
| Prof-school|             Other|          4|
| Prof-school|             White|        514|
|Some-college|Amer-Indian-Eskimo|         79|
|Some-college|Asian-Pac-Islander|        208|
|Some-college|             Black|        746|
|Some-college|             Other|         51|
|Some-college|             White|       6207|
+------------+------------------+-----------+



val newFormattedRDD = filteredRDD.map(line => line.replace(" ", "").split(",")).map(x => Row(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toInt, x(5), x(6), x(7), x(8), x(9), x(10).toInt, x(11).toInt, x(12).toInt, x(13), x(14)))
             
scala> newFormattedRDD.map(line => ((line(3), line(8)),1)).reduceByKey((x,y)=> x+y).sortByKey().foreach(println)
((10th,Amer-Indian-Eskimo),16)
((10th,Asian-Pac-Islander),13)
((10th,Black),133)
((10th,Other),9)
((10th,White),762)
((11th,Amer-Indian-Eskimo),14)
((11th,Asian-Pac-Islander),21)
((11th,Black),153)
((11th,Other),10)
((11th,White),977)
((12th,Amer-Indian-Eskimo),5)
((12th,Asian-Pac-Islander),9)
((12th,Black),70)
((12th,Other),14)
((12th,White),335)
((1st-4th,Amer-Indian-Eskimo),4)
((1st-4th,Asian-Pac-Islander),5)
((1st-4th,Black),16)
((1st-4th,Other),9)
((1st-4th,White),134)
((5th-6th,Amer-Indian-Eskimo),2)
((5th-6th,Asian-Pac-Islander),18)
((5th-6th,Black),21)
((5th-6th,Other),13)
((5th-6th,White),279)
((7th-8th,Amer-Indian-Eskimo),9)
((7th-8th,Asian-Pac-Islander),11)
((7th-8th,Black),56)
((7th-8th,Other),17)
((7th-8th,White),553)
((9th,Amer-Indian-Eskimo),5)
((9th,Asian-Pac-Islander),9)
((9th,Black),89)
((9th,Other),8)
((9th,White),403)
((Assoc-acdm,Amer-Indian-Eskimo),8)
((Assoc-acdm,Asian-Pac-Islander),29)
((Assoc-acdm,Black),107)
((Assoc-acdm,Other),8)
((Assoc-acdm,White),915)
((Assoc-voc,Amer-Indian-Eskimo),19)
((Assoc-voc,Asian-Pac-Islander),38)
((Assoc-voc,Black),112)
((Assoc-voc,Other),6)
((Assoc-voc,White),1207)
((Bachelors,Amer-Indian-Eskimo),21)
((Bachelors,Asian-Pac-Islander),289)
((Bachelors,Black),330)
((Bachelors,Other),33)
((Bachelors,White),4682)
((Doctorate,Amer-Indian-Eskimo),3)
((Doctorate,Asian-Pac-Islander),28)
((Doctorate,Black),11)
((Doctorate,Other),2)
((Doctorate,White),369)
((HS-grad,Amer-Indian-Eskimo),119)
((HS-grad,Asian-Pac-Islander),226)
((HS-grad,Black),1174)
((HS-grad,Other),78)
((HS-grad,White),8904)
((Masters,Amer-Indian-Eskimo),5)
((Masters,Asian-Pac-Islander),88)
((Masters,Black),86)
((Masters,Other),7)
((Masters,White),1537)
((Preschool,Asian-Pac-Islander),6)
((Preschool,Black),5)
((Preschool,Other),2)
((Preschool,White),38)
((Prof-school,Amer-Indian-Eskimo),2)
((Prof-school,Asian-Pac-Islander),41)
((Prof-school,Black),15)
((Prof-school,Other),4)
((Prof-school,White),514)
((Some-college,Amer-Indian-Eskimo),79)
((Some-college,Asian-Pac-Islander),208)
((Some-college,Black),746)
((Some-college,Other),51)
((Some-college,White),6207)
