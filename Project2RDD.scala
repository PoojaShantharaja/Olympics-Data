package com.spark.practice.RDD

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window

object Project2RDD {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Olympics")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.textFile("resources/OlympicData_Cleaned.csv")

    //KPI 1 : No  of  athletes  participated  in  each  Olympic  event

    val NumAthletesPerSportRDD = data.map(field => {
      var x = (field.split(",")(5).toString(), field.split(",")(0).toString())
      x
    })
    //println("Group Started")
    val a = NumAthletesPerSportRDD.groupByKey().sortBy(_._1)
    //a.foreach(println)

    a.foreach(x => {

      var len = x._2.size
      var count = 0

      for (i <- 0 to len) {
        count += 1
      }

      println("Sports : " + x._1 + "===> Count : " + count)
    })

    // KPI 2 : No  of  medals  each  country  won  in  each  Olympic  in  ascending  order

    val MedalsPerCountryPerYearRDD = data.map(field => {
      var x = ((field.split(",")(2).toString, field.split(",")(3).toInt), field.split(",")(9).toInt)
      x
    })

    val res2 = MedalsPerCountryPerYearRDD
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = true) //.collect()
    println("\n No.  of  medals  each  country  won  in  each  Olympic  in  ascending  order :\n")
    res2.foreach(println)

    // KPI 3: Top  10  athletes  who  won  highest  gold  medals in  all  the  Olympic  events

    val topTenAthleteGold = data.map(field => {
      var x = ((field.split(",")(0).toString(), field.split(",")(5).toString()), field.split(",")(6).toInt)
      x
    })

    val res3 = topTenAthleteGold.reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n Top  10  athletes  who  won  highest  gold  medals " +
      "in  all  the  Olympic  events:\n")
    res3.take(10).foreach(println)

    // KPI4: No  of  athletes  who  won  gold  and  whose  age  is  less  than  20

    val filteredGoldAndAgeLessThan20 = data.filter(field => {
      field.split(",")(1).toInt < 20 && field.split(",")(6).toInt >= 1
    })

    //filteredGoldAndAgeLessThan20.take(15).foreach(println)

    val res4 = filteredGoldAndAgeLessThan20.map(field => {
      var x = ((field.split(",")(0).toString(), field.split(",")(1).toInt),
        field.split(",")(6).toInt)
      x
    })
      .groupByKey()
      .count()

    println("\n No. of Athletes with Age<20 and won Gold :\n" + res4)


    // KPI 6: No  of  atheletes  from  each  country  who  has  won  a  medal  in  each  Olympic  in  each  sports

    val filteredMedal = data.filter(field => {
      var x = field.split(",")(9).toInt >= 1
      x
    })

    val temp6 = filteredMedal.map(field => {
      var x = ((field.split(",")(2).toString, field.split(",")(3).toInt, field.split(",")(5).toString), field.split(",")(0).toString)

      x
    })
    val res6 = temp6.groupByKey().sortBy(_._1, ascending = true)

    print("\n No  of  atheletes  from  each  country  who  has " +
      "won  a  medal  in  each  Olympic  in  each  sports : \n")

    res6.foreach(x => {

      var len = x._2.size
      var count = 0

      for (i <- 0 to len) {
        count += 1
      }

      println(x._1 + ": Count = " + count)
    })

    // KPI 7: No  of  athletes  won  at  least  a medal  in  each  event  in  all  the  Olympics

    val medal = data.filter(field => {
      var x = field.split(",")(9).toInt >= 1
      x
    })

    val temp7 = medal.map(field => {
      val x = (field.split(",")(5).toString, field.split(",")(0).toString)
      x
    })

    val res7 = temp7.groupByKey().sortBy(_._1, ascending = true)

    println("\n No  of  athletes  won  at  least  a medal "
      + "in  each  event  in  all  the  Olympics : \n")

    res7.foreach(x => {

      var len = x._2.size
      println(x._1 + ":" +len)
     
    })

    // KPI 8: Country  won  highest  no  of  medals  in  wrestling in  2012

    println("Country  won  highest  no  of  medals  in  wrestling in  2012 :" + "\n")
    val filteredWrestling = data.filter(field => {
      var x = (field.split(",")(3).toInt == 2012 &&
        field.split(",")(5).toString().toLowerCase() == "wrestling")
      x
    })

    val res8 = filteredWrestling.map(field => {
      val x = (field.split(",")(2).toString(), field.split(",")(9).toInt)
      x

    }).reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1).foreach(println)

  }

}