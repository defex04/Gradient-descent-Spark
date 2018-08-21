import functions.LoadData;
import functions.MiniBatchGradientDescent;
import functions.PropertiesFromFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.IOException;


class Main {
    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        PropertiesFromFile.setPropertiesFromFile("src/main/resources/properties.cfg");

        JavaPairRDD<Double[], Double> numbersRDD = LoadData.loadData(sc,
                PropertiesFromFile.DATA_PATH);

        numbersRDD.persist(StorageLevel.MEMORY_ONLY());

        long startTime = System.currentTimeMillis();
        Double[] thetas = MiniBatchGradientDescent.findParameters(sc, numbersRDD, PropertiesFromFile.DATA_SIZE,
                PropertiesFromFile.THETAS_AMOUNT, PropertiesFromFile.ALPHA, PropertiesFromFile.MAX_ITERATION,
                PropertiesFromFile.THRESHOLD);
        long timeSpent = System.currentTimeMillis() - startTime;

        for (Double theta : thetas) {
            System.out.println(theta);
        }
        System.out.println("Метод выполнялся " + timeSpent + " миллисекунд");
    }
}
