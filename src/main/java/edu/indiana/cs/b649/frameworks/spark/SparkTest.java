package edu.indiana.cs.b649.frameworks.spark;

import java.util.Arrays;
import java.util.List;

public class SparkTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("HelloWorld");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        int x = distData.reduce((i,j) -> i+j);
        System.out.println(x);
    }
}
