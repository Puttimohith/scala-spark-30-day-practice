import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 6 - Word Count")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    println("=== Day 6 - Word Count ===")

    // Read application logs
    val linesRDD = sc.textFile("data/logs.txt")

    println("\n=== Input Logs ===")
    linesRDD.collect().foreach(println)

    // flatMap: split each line into individual words
    val wordsRDD = linesRDD.flatMap { line =>
      line
        .replaceAll("[^a-zA-Z0-9\\s]", "")
        .toLowerCase
        .split("\\s+")
        .filter(_.nonEmpty)
    }

    println("\n=== Words ===")
    println(wordsRDD.collect().mkString(", "))

    // map: convert each word into (word, 1)
    val wordPairsRDD = wordsRDD.map(word => (word, 1))

    println("\n=== Word Pairs ===")
    println(wordPairsRDD.collect().mkString(", "))

    // reduceByKey: add counts for the same word
    val wordCountsRDD = wordPairsRDD.reduceByKey(_ + _)

    println("\n=== Word Counts ===")
    wordCountsRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // Find the top 10 most frequent words
    val top10Words = wordCountsRDD
      .map { case (word, count) => (count, word) }
      .sortByKey(ascending = false)
      .take(10)

    println("\n=== Top 10 Most Frequent Words ===")

    top10Words.foreach {
      case (count, word) =>
        println(s"$word -> $count")
    }

    println("\n=== Word Count Flow ===")
    println("flatMap -> Split lines into words")
    println("map -> Convert words into (word, 1)")
    println("reduceByKey -> Add counts for each word")

    println("\n=== Case-Insensitive and Punctuation Handling ===")
    println("Words are converted to lowercase.")
    println("Punctuation is removed.")
    println("Empty words are ignored.")

    sc.stop()

    println("\n=== Application Completed Successfully ===")
  }
}