/**
  * Created by yogi on 2016/9/23.
  */
import java.io.FileWriter
import java.io.File

import scala.util.Random
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*
object SampleDataFileGenerator {
	def main(args:Array[String]){
		val writer = new FileWriter(new File("D:\\CAT\\z\\sample_age_dat.txt"), false)
		val rand = new Random()
	  	for(i <- 1 to 10000000) {

			writer.write(i + " " + rand.nextInt(100))
		  	writer.write(System.getProperty("line.separator"))
		}

	  	writer.flush();
	  	writer.close();
	}
}


object AvgAgeCalculator{

  def main(args:Array[String]){

	val conf = new SparkConf().setAppName("Spark Average Age Calculator").setMaster("local")
	val sc = new SparkContext(conf)
	val dataFile = sc.textFile("D:\\CAT\\z\\sample_age_dat.txt")
	val count = dataFile.count()
	val ageData = dataFile.map(line => (line.split(" ")(1)))
	val totalAge = ageData.map(age => Integer.parseInt(String.valueOf(age))).collect().reduce((a, b) => a + b)
	println("Total Age:" + totalAge + "; Number of People:" + count)
	val avgAge : Double = totalAge.toDouble / count.toDouble
	println("Average Age is " + avgAge)

  }
}


object PeopleInfoFileGenerator {

  def main(args:Array[String]) {
	val writer = new FileWriter(new File("D:\\CAT\\z\\sample_people_info.txt"), false)
	val rand = new Random()
	for(i <- 1 to 100000){
		var height = rand.nextInt(200)
	  	if(height < 50){
		  height = height + 50
		}
	   var gender = getRandomGender
	   if(height < 100 && gender == "M")
		 height = height + 100
	  if(height < 100 && gender == "F")
		 height = height + 50
	  writer.write(i + " " + gender + " " + height)
	  writer.write(System.getProperty("line.separator"))

	}
	writer.flush()
	writer.close()
	println("People Information FIle generated successfully.")
  }

  def getRandomGender():String ={

	val rand = new Random()
	val randNum = rand.nextInt(2) + 1
	if(randNum % 2 == 0)
	  {
		"M"
	  }else
	  {
		"F"
	  }
  }

}


object PeopleInfoCalcular{
  def main(args:Array[String])
  {
	val conf = new SparkConf().setAppName("PeopleInfoCalcular").setMaster("local")
	val sc = new SparkContext(conf)

	val dataFile = sc.textFile("D:\\CAT\\z\\sample_people_info.txt")
	val maleData = dataFile.filter(line => line.contains("M")).map(line => (line.split(" ")(1) + " " + line.split(" ")(2)))
	val femaleData = dataFile.filter(line => line.contains("F")).map(line => (line.split(" ")(1) + " " + line.split(" ")(2)))
	val maleHeightData = maleData.map(line => line.split(" ")(1).toInt)
	val femaleHeightData = femaleData.map(line =>line.split(" ")(1).toInt)

	val lowestMale = maleHeightData.sortBy(x => x, true).first()
	val lowestFemale = femaleHeightData.sortBy(x => x, true).first()
	val highestMale  = maleHeightData.sortBy(x => x,false).first()
	val highestFemale = femaleHeightData.sortBy(x => x,false).first()
	println("Number of Male Peole:" + maleData.count())
	println("Number of Female Peole:" + femaleData.count())
	println("Lowest Male:" + lowestMale)
	println("Lowest Female:" + lowestFemale)
	println("Highest Male:" + highestMale)
	println("Highest Female:" + highestFemale)

  }


object TopkSearchKeyWords{

  def main(arg:Array[String])
  {
	val conf = new SparkConf().setAppName("TopkSearchKeyWords").setMaster("local")
	val sc = new SparkContext(conf)
	val dataFile = sc.textFile("D:\\CAT\\z\\HeroAttrSet.json")
	val keyCount = dataFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
	val sortedData = keyCount.map{case (k, v) => (v, k)}.sortByKey(false)
	val topkData = sortedData.take(10).map{case (v, k) => (k, v)}
	topkData.foreach(println)
  }
*/

object PeopleDataStatistics2{
  private val schemaString = "id,gender,height"
  def main(args:Array[String]){

	val conf = new SparkConf().setAppName("PeopleDataStatistics").setMaster("local")
	val sc = new SparkContext(conf)
	val dataRDD = sc.textFile("D:\\CAT\\z\\sample_people_info.txt")
	val sqlCtx = new SQLContext(sc)
	import sqlCtx.implicits._
	val schemaArray = schemaString.split(",")
	val schema = StructType(schemaArray.map(fiedldName => StructField(fiedldName, StringType, true)))
	val rowRDD: RDD[Row] = dataRDD.map(_.split(" ")).map(eachRow => Row(eachRow(0), eachRow(1), eachRow(2)))
	val peopleDF = sqlCtx.createDataFrame(rowRDD, schema)
	peopleDF.registerTempTable("people")
	val higherMale180 = sqlCtx.sql("select id,gender,height from people where height > 170 and gender='M'")
	println("Men whose height are more then 180:" + higherMale180.count())
	println("<Display #1>")
	val higherFeamle170 = sqlCtx.sql("select id,gender,height from people where height > 170 and gender='F'")
	println("women whose height are more than 170:" + higherFeamle170.count())
	println("<Display #2")
	peopleDF.groupBy(peopleDF("gender")).count().show()
	println("People Count Grouped By Gender")
	println("Display #3")
	peopleDF.filter(peopleDF("gender").equalTo("M")).filter(peopleDF("height") > 210).show(50)
	println("<Display #4>")
	peopleDF.sort($"height".desc).take(50).foreach{row => println(row(0) + "," + row(1) + "," + row(2))}
	println(" <Display #5>")
	peopleDF.filter(peopleDF("gender").equalTo("M")).agg(Map("height" -> "avg")).show()

  }


}