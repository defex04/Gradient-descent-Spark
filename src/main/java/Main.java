import functions.MiniBatchGradientDescent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;



class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("src/main/resources/sample.txt");

        JavaPairRDD<Double[], Double> numbersRDD = lines.mapToPair(
                s -> {
                    String[] splitString = s.split(" ");
                    Double y = Double.parseDouble(splitString[splitString.length - 1]);
                    Double[] x = new Double[1];

                    for (int i =0; i < 1; i++) {
                        x[i] = Double.parseDouble(splitString[i]);
                    }
                    return new Tuple2<>(x, y);
                }
        );

        numbersRDD.persist(StorageLevel.MEMORY_ONLY());

        long startTime = System.currentTimeMillis();

        Double[] thetas = MiniBatchGradientDescent.findParameters(sc, numbersRDD, 731,
                2,0.1, 1000, 0);

        long timeSpent = System.currentTimeMillis() - startTime;


        for (Double theta:thetas) {
            System.out.println(theta);
        }

        System.out.println("Метод выполнялся " + timeSpent + " миллисекунд");

    }


}
