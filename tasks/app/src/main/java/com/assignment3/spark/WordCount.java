package com.assignment3.spark;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkFiles;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;

public class WordCount {

    static class Filter implements FlatMapFunction<String, String>
    {
        @Override
        public Iterator<String> call(String s) {
            /*
             * add your code to filter words
             */
            String[] subStrings = s.split("\\s+");
            return Arrays.asList(subStrings).iterator();
        }

    }

    public static void main(String[] args) {
        String textFilePath = "input/pigs.txt"; // update to HDFS url for task2
        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("local[*]"); // task2: update the setMaster with your cluster master URL for executing this code on the cluster
        JavaSparkContext sparkContext =  new JavaSparkContext(conf);
        JavaRDD<String> textFile = sparkContext.textFile(textFilePath);
        JavaRDD<String> words = textFile.flatMap(new Filter());

        /*
         * add your code for key value mapping
         *
         * add your code to perform reduce on the given key value pairs
         *
         * print the word count and save the output in the format, e.g.,(in:15) to an 'output' folder (on HDFS for task 2)
         * try to consolidate your output into single text file if you want to check your output against the given sample output
         */

        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b);
        counts = counts.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
        //counts.saveAsTextFile("hdfs://172.20.10.2:9000/sparkApp/input/output.txt");
        //saves the partitions as one file
        counts.coalesce(1).saveAsTextFile("hdfs://172.20.10.2:9000/sparkApp/input/output.txt")



        sparkContext.stop();
        sparkContext.close();
    }
}

