import spark.apache.sql.types._

val mySchema = StructType(Array(
        StructField("pcode", StringType),
        StructField("lastName", StringType),
        StructField("firstName", StringType),
        StructField("age", IntegerType)
    ))

val peopleRDD = sc.textFile("people.csv").map(line => line.split(",")).map(x => Row(x(0), x(1), x(2), x(3).toInt))

val peopleDF = sqlContext.createDataFrame(peopleRDD, mySchema)




//python

from spark.apache.sql import *

mySchema = StructType([
        StructField("pcode", StringType()),
        StructField("lastName", StringType()),
        StructField("firstName", StringType()),
        StructField("age", IntegerType())
    ])

peopleRdd = sc.textFile("people.csv").map(lambda line: line.split(",")).map(lambda x: [x[0], x[1], x[2], int(x[3])])

peopleDF = sqlContext.createDataFrame(peopleRDD, mySchema)