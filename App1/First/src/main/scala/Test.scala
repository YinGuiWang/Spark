/**
	* Created by yogi on 2016/8/4.
	*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkWordCount{
  def FILE_NAME:String = "word_count_result_"
  def main(args:Array[String]){
	if(args.length < 1){
	  	println("Usage:SparkWordCount FileName");
		System.exit(1);
	}
	val conf = new SparkConf().setAppName("Spark exercise: Spark Version Word Count Program").setMaster("local");
	val sc = new SparkContext(conf)
	val textFile = sc.textFile("D:\\CAT\\z\\HeroAttrSet.json");
	val wordCounts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
	//wordCounts.saveAsTextFile(FILE_NAME+System.currentTimeMillis());
	wordCounts.foreach(wordNumberPair => println(wordNumberPair._1 + ":" + wordNumberPair._2))
	sc.stop();
  }


}
