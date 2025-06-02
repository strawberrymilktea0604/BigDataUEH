package vn.edu.ueh.bit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

public class WordCounter {
    private static void wordCount(String inputFile, String outputFile) {
        SparkConf conf = new SparkConf()
                .setAppName("Word Counter") // [cite: 4, 17]
                .setMaster("local") // 1 JVM. [cite: 4]
                // Use local [*] if you want to use multiple JVMs as much as possible [cite: 5]
                ;
        try (JavaSparkContext sc = new JavaSparkContext(conf)) { // [cite: 6, 17]
            JavaRDD<String> lines = sc.textFile(inputFile); // [cite: 6, 18]

            JavaRDD<String> words = lines
                    .flatMap(line -> Arrays.asList(line.split(" ")) // [cite: 6]
                            .iterator()); // [cite: 6]

            JavaPairRDD<String, Integer> pairs = words
                    .mapToPair(word -> new Tuple2<>(word, 1)); // [cite: 7]

            JavaPairRDD<String, Integer> counts = pairs
                    .reduceByKey(Integer::sum); // $//a+b$ [cite: 7]

            counts = counts.sortByKey(); // [cite: 7]
            counts.saveAsTextFile(outputFile); // save results [cite: 8]
        }
    }

    public static void main(String[] args) throws Exception {
        //
        // String inputFile = "src/main/resources/input.txt"; // [cite: 9]
        //
        // String outputFolder = "src/main/resources/out"; // [cite: 9]

        String inputFile = "hdfs://localhost:9000/BigData/input.txt"; // [cite: 9]
        String outputFolder = "hdfs://localhost:9000/output"; // [cite: 9]

        wordCount(inputFile, outputFolder); // [cite: 9]
    }
}
