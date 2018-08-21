import functions.LoadData;
import functions.MiniBatchGradientDescent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;



class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[64]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Double[], Double> numbersRDD = LoadData.loadData(sc,
                "src/main/resources/sample_big.txt");

        numbersRDD.persist(StorageLevel.MEMORY_ONLY());

        long startTime = System.currentTimeMillis();

        Double[] thetas = MiniBatchGradientDescent.findParameters(sc, numbersRDD, 1720482,
                2,0.1, 500, 0);

        long timeSpent = System.currentTimeMillis() - startTime;


        for (Double theta:thetas) {
            System.out.println(theta);
        }

        System.out.println("Метод выполнялся " + timeSpent + " миллисекунд");

    }


}
