package com.spark.practice.DF

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.sources
import org.apache.spark.sql.expressions.Window

object Project2DF {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Olympics")
      .config("spark.master", "local")
      .getOrCreate()
    //val conf = new SparkConf().setAppName("PatientHospitalDetails").setMaster(master)

    import spark.implicits._

    val data = spark.read.textFile("resources/olympic_Data.csv")

    val rdd = data.map(field => {
      val Athlete = field.split(",")(0).trim().toString
      val Age = field.split(",")(1).trim().toString
      val Country = field.split(",")(2).trim().toString
      val Year = field.split(",")(3).trim().toString
      val Date = field.split(",")(4).trim().toString
      val Sport = field.split(",")(5).trim().toString
      val Gold = field.split(",")(6).trim().toString
      val Silver = field.split(",")(7).trim().toString
      val Bronze = field.split(",")(8).trim().toString
      val Total = field.split(",")(9).trim().toString

      val case1 = (Athlete, Age, Country, Year, Date, Sport, Gold, Silver, Bronze, Total)
      case1
    })

    var df = rdd.toDF("Athlete", "Age", "Country", "Year", "Date", "Sport", "Gold", "Silver", "Bronze", "Total")

    println("===> Current DF count: " + df.count())
    //.show(9000)

    // Filter the null/empty values

    /* 
    // df = df.withColumn("Athlete", df("Athlete").cast(toString)).withColumn("Age", df("Age").cast(toInt))
  
    //df = df.na.drop()
    //println(df.count())
    
   
    
    //var filtered = df.filter(row -> row.anyNull());

   // df = df.filter(df("Age") !== "") //|| Age != "" )
   // println(df.count())
   
      df = df.filter( df("Athlete") !== "")
    df = df.filter( $"Age" !== "")
    println(df.count())
     df.show()
 */

    /*   This works in Spark 1.6
    df = df.filter(df("Athlete").isNotNull || df("Athlete") !== "")
    df = df.filter($"Age".isNotNull || $"Age" !== "")
    df = df.filter(df("Country").isNotNull || df("Country") !== "")
    df = df.filter(df("Year").isNotNull || df("Year") != "")
    df = df.filter(df("Date").isNotNull || df("Date") != "")
    df = df.filter(df("Sport").isNotNull || df("Sport") != "")
    df = df.filter(df("Gold").isNotNull || df("Gold") != "")
    df = df.filter(df("Silver").isNotNull || df("Silver") != "")
    df = df.filter(df("Bronze").isNotNull || df("Bronze") != "")
    df = df.filter(df("Total").isNotNull || df("Total") != "")
   
    println(df.count())
    
*/

    /* var condition = df.columns.map(x=> col(x).isNull || col(x) === "").reduce(_ || _)
    println("Condition ==> "+condition)
    println("condition count ==> "+df.filter(condition).count())
    var nullDF = df.filter(condition)
    
    var filteredDf = df.except(nullDF)
    println("Filter DF Count ===>" + filteredDf.count())
    
    */

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
    // df.show()

    // KPI 1: No  of  athletes  participated  in  each  Olympic  event

    println("No  of  athletes  participated  in  each  Olympic  event :\n")

    var numAthletesInEachEvent = df.groupBy("Sport")
      .count()
      .withColumnRenamed("count", "CountofAthletesPerEvent")
      .sort("Sport")
      .show()

    //KPI 2 :No  of  medals  each  country  won  in  each  Olympic  in  ascending  order

    println("No  of  medals  each  country  won  in  each  Olympic :\n")

    var medalsPerCountryPerYear = df.groupBy("Country", "Year")
      .sum("Total")
      .withColumnRenamed("sum(Total)", "totalMedalsPerCountry")
      .sort("totalMedalsPerCountry")
      .show()

    //KPI 3:Top  10  athletes  who  won  highest  gold  medals in  all  the  Olympic  events

    println("Top  10  athletes  who  won  highest  gold  medals :\n")

    var highestGold = df.groupBy("Athlete")
      .sum("Gold")
      .withColumnRenamed("sum(Gold)", "sumGold")
      .sort(desc("sumGold"))
      .show(10)
    //highestGold.show(10)

    // KPI 4: No  of  athletes  who  won  gold  and  whose  age  is  less  than  20

    var res4 = df.filter($"Age" < 20 && $"Gold" >= 1)
      .groupBy("Athlete")
      .sum("Total")
      .count()

    println("\n No  of  athletes  who  won  gold  and  whose  age  is  less  than  20 : \n" + res4)

    //KPI 5 :Youngest  athlete  who  won  gold  in  each  category  of  sports  in  each  Olympic

    println("Youngest  athlete  who  won  gold  in  each  category  of  sports  in  each  Olympic :\n")
    /*   
    val youngestGold = df.filter($"Gold" >= 1)
    .groupBy("Sport", "Age", "Year", "Athlete")
    .sum("Gold").withColumnRenamed("sum(Gold)", "sumGold")
    .sort("Age")
    .show()
   */

    val partitionWindow = Window.partitionBy("Year").orderBy(asc("Age"))
    val youngestGold = df.filter($"Gold" >= 1)
      .select("Athlete", "Age", "Year", "Sport", "Gold")
      .withColumn("rank", rank().over(partitionWindow))
      .where($"rank" === 1)
      .sort(asc("Age"), asc("Year"))
      .show()

    // KPI 6 :No.  of  athletes  from  each  country  who  has  won  a  medal  in  each  Olympic  in  each  sports

    println("\nNo.  of  athletes  from  each  country  who  has  won  a  medal "
      + "in  each  Olympic  in  each  sports : \n")
    val res6 = df.filter($"Total" >= 1)
      .groupBy("Country", "Year", "Sport")
      .count()
      .orderBy("Country": String, "Year", "count")
      .show()

    // KPI 7 : No.  of  athletes  won  at  least  a medal  in  each  event  in  all  the  Olympics

    println("\n No.  of  athletes  won  at  least  a medal  in  each  event  in  all  the  Olympics : \n")
    val res7 = df.filter($"Total" >= 1)
      .groupBy("Sport")
      .count()
      .orderBy("Sport")
      .show()

    // KPI 8 : Country  won  highest  no  of  medals  in  wrestling in  2012

    println("Country  won  highest  no  of  medals  in  wrestling in  2012 : \n")

    val res8 = df.filter(lower($"Sport") === "wrestling" && $"Year" === 2012)
      .groupBy("Country")
      .sum("Total").withColumnRenamed("sum(Total)", "totalMedals")
      .sort(desc("totalMedals"))
      .show(1)

  }
}