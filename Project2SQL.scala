package com.spark.practice.SparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Project2SQL {

  case class Olympic(Athlete: String, Age: String, Country: String, Year: String, Date: String,
    Sport: String, Gold: String, Silver: String, Bronze: String, Total: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("OlympicsSQL")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    import spark.sql
    //import sqlContext.implicits._
    val data = spark.read.textFile("resources/olympic_Data.csv")

    val rdd = data.map(field => {
      val x = field.split(",")
      Olympic(x(0).trim().toString, x(1).trim().toString, x(2).trim().toString, x(3).trim().toString, x(4).trim().toString,
        x(5).trim().toString, x(6).trim().toString, x(7).trim().toString, x(8).trim().toString, x(9).trim().toString)

    })

    var df = rdd.toDF("Athlete", "Age", "Country", "Year", "Date", "Sport", "Gold", "Silver", "Bronze", "Total")

    df.printSchema()
    //df.show()

    println("===> Current DF count: " + df.count())
    // Filter the null/empty values

    df = df.filter(!df("Athlete").equalTo(""))
    df = df.filter(!df("Age").equalTo(""))
    df = df.filter(!df("Country").equalTo(""))
    df = df.filter(!df("Year").equalTo(""))
    df = df.filter(!df("Date").equalTo(""))
    df = df.filter(!df("Sport").equalTo(""))
    df = df.filter(!df("Gold").equalTo(""))
    df = df.filter(!df("Silver").equalTo(""))
    df = df.filter(!df("Bronze").equalTo(""))
    df = df.filter(!df("Total").equalTo(""))

    println("===> filtered DF count : " + df.count())

    //udf to convert string values to integer

    def toInt = udf((colValue: String) => {
      colValue.toInt
    })

    df = df.withColumn("newYear", toInt(df("Year")))
      .withColumn("newAge", toInt(df("Age")))
      .withColumn("newGold", toInt(df("Gold")))
      .withColumn("newSilver", toInt(df("Silver")))
      .withColumn("newBronze", toInt(df("Bronze")))
      .withColumn("newTotal", toInt(df("Total")))
      .drop("Age").withColumnRenamed("newAge", "Age")
      .drop("Year").withColumnRenamed("newYear", "Year")
      .drop("Gold").withColumnRenamed("newGold", "Gold")
      .drop("Silver").withColumnRenamed("newSilver", "Silver")
      .drop("Bronze").withColumnRenamed("newBronze", "Bronze")
      .drop("Total").withColumnRenamed("newTotal", "Total")

    df.printSchema()
    //df.show()

    df.createOrReplaceTempView("OlympicsDF")

    //val sample = spark.sql("select * from OlympicsDF limit 10").show()     

    //KPI 1 : No.  of  athletes  participated  in  each  Olympic  event

    println("No  of  athletes  participated  in  each  Olympic  event :\n")
    val res1 = spark.sql("select count(Athlete) as cnt, sport from OlympicsDF group by sport order by cnt desc") //.withColumnRenamed("count", "countPerEvent")
      .show()

    // KPI 2 : No  of  medals  each  country  won  in  each  Olympic  in  ascending  order

    println("No  of  medals  each  country  won  in  each  Olympic :\n")
    val res2 = spark.sql("select country, year, sum(Total) as totalMedals from OlympicsDF "
      + "group by Country,year order by totalMedals asc")
      .show()

    // KPI 3 : Top  10  athletes  who  won  highest  gold  medals in  all  the  Olympic  events

    println("Top  10  athletes  who  won  highest  gold  medals :\n")
    val res3 = spark.sql("select Athlete, sum(Gold) as goldSum from OlympicsDF "
      + "group by Athlete order by goldSum desc limit 10")
    res3.show()

    // KPI 4 : No  of  athletes  who  won  gold  and  whose  age  is  less  than  20

    println("\n No  of  athletes  who  won  gold  and  whose  age  is  less  than  20 : \n")
    val res4 = spark.sql("select count(Athlete) as athleteCount from OlympicsDF where Gold >=1 and Age < 20")
      .show()

    // KPI 5: Youngest  athlete  who  won  gold  in  each  category  of  sports  in  each  Olympic

    println("Youngest  athlete  who  won  gold  in  each  category  of  sports  in  each  Olympic :\n")
    val res5 = spark.sql("select Athlete,Age, Sport, Year  from OlympicsDF where Gold >=1 group by Athlete, Sport, Year,Age order by Age limit 1")
      .show()

    // KPI 6 : No  of  atheletes  from  each  country  who  has  won  a  medal  in  each  Olympic  in  each  sports

    println("\nNo.  of  athletes  from  each  country  who  has  won  a  medal "
      + "in  each  Olympic  in  each  sports : \n")
    val res6 = spark.sql("select count(Athlete) as athleteCount, country,year, sport from OlympicsDF "
      + "where Total >= 1 group by Country,sport,year order by country, year, athleteCount desc")
      .show()

    // KPI 7 : No  of  athletes  won  at  least  a medal  in  each  event  in  all  the  Olympics

    println("\n No.  of  athletes  won  at  least  a medal  in  each  events  in  all  the  Olympics : \n")
    val res7 = spark.sql("select count(Athlete) as cnt, Sport from OlympicsDF where Total >=1 group by Sport order by sport")
      .show()

    // KPI 8 : Country  won  highest  no  of  medals  in  wrestling in  2012

    println("Country  won  highest  no  of  medals  in  wrestling in  2012 : \n")
    val res8 = spark.sql("select country, sum(Total) as totalMedals from OlympicsDF "
      + "where lower(sport) = 'wrestling' and  year == 2012 group by country order by totalMedals desc limit 1")
      .show()

  }

}