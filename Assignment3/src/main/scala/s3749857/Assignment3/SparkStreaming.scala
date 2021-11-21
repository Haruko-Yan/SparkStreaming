package s3749857.Assignment3

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ArrayBuffer

object SparkStreaming {
  
  def updateFunc(newValues: Seq[Int], oldValue: Option[Int]): Option[Int] = {
    Option(newValues.sum + oldValue.getOrElse(0))
  }
  
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: HdfsWordCount <task name> <input> <output>")
      System.exit(1)
    }
    
    args(0) match {
      case "taskA" => taskA(args(1), args(2))
      case "taskB" => taskB(args(1), args(2))
      case "taskC" => taskC(args(1), args(2))
      case _ => println("The task name must be 'taskA', 'taskB' or 'taskC'!")
    }
  }
  
  def taskA(inputPath: String, outputPath: String): Unit = {
    // Create the spark configuration
    val sparkConf = new SparkConf().setAppName("TaskA").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(5))
 
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(inputPath) // get the context from new files in the input directory
    val words = lines.flatMap(_.split(raw"\s")) // split every line by space 
    val pattern = raw"^[a-zA-Z]+$$".r // the pattern means that each word only contains characters
    val filtered = words.filter(word => pattern.findAllIn(word).nonEmpty) // filter out the token which contains something other than characters
    val wordCounts = filtered.map(x => (x, 1)).reduceByKey(_ + _) // map every word into (k,v) pair and count the numbers of same words
    wordCounts.saveAsTextFiles(outputPath)
    ssc.start()
    ssc.awaitTermination()
  }
  
  def taskB(inputPath: String, outputPath: String) {
    // Create the spark configuration
    val sparkConf = new SparkConf().setAppName("TaskB").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(5))
 
    // Create the FileInputDStream on the directory and use the
    // stream to count co-occurred word pairs in new files created
    val pattern = raw"^[a-zA-Z]+$$".r // the pattern means that each word only contains characters
    val lines = ssc.textFileStream(inputPath) // get the context from new files in the input directory
    val pairs = lines.flatMap(line => {
      val words = line.split(raw"\s+") // split every line by space
      val filteredWord = words.filter(word => word.length() >= 5 && pattern.findAllIn(word).nonEmpty) // filter out the token which contains something other than characters or whose length is less than 5
      val arr = new ArrayBuffer[((String, String), Int)]() // for storing the key-value pairs
      // Traverse all co-occurred words with no replacement
      for (i <- 0 to filteredWord.length - 1) {
        for (j <- i + 1 to filteredWord.length - 1 if filteredWord(i) != filteredWord(j)) { // the pair with same words will not be added into the array
          arr += (((filteredWord(i), filteredWord(j)), 1))
        }
      }
      arr
    })
    
    val wordCounts = pairs.reduceByKey(_ + _) // map every word into (k,v) pair and count the numbers of same word pairs
    wordCounts.saveAsTextFiles(outputPath)
    ssc.start()
    ssc.awaitTermination()
  }
  
  def taskC(inputPath: String, outputPath: String){
    // Create the spark configuration
    val sparkConf = new SparkConf().setAppName("TaskC").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    
    // set check point directory
    ssc.checkpoint("s3749857/checkpoint")
 
    // Create the FileInputDStream on the directory and use the
    // stream to continuously count co-occurred word pairs in new files created
    val pattern = raw"^[a-zA-Z]+$$".r // the pattern means that each word only contains characters
    val lines = ssc.textFileStream(inputPath) // get the context from new files in the input directory
    val pairs = lines.flatMap(line => {
      val words = line.split(raw"\s+") // split every line by space
      val filteredWord = words.filter(word => word.length() >= 5 && pattern.findAllIn(word).nonEmpty) // filter out the token which contains something other than characters or whose length is less than 5
      val arr = new ArrayBuffer[((String, String), Int)]() // for storing the key-value pairs
      // Traverse all co-occurred words with no replacement
      for (i <- 0 to filteredWord.length - 1) {
        for (j <- i + 1 to filteredWord.length - 1 if filteredWord(i) != filteredWord(j)) { // the pair with same words will not be added into the array
          arr += (((filteredWord(i), filteredWord(j)), 1))
        }
      }
      arr
    })
    
    val wordCounts = pairs.updateStateByKey(updateFunc) // continuously update the co-occurrence frequency of words
    wordCounts.saveAsTextFiles(outputPath)
    ssc.start()
    ssc.awaitTermination()
  }
}