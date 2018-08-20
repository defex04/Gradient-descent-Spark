import functions.GradientDescent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;


public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\andre\\Desktop\\Gradient-descent-Spark\\src\\main\\java\\sample3.txt");

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
        //numbersRDD.collect().forEach(tuple -> { for(Double d : tuple._1()) System.out.print(d + " "); System.out.println(tuple._2()); });


        //System.out.println(numbersRDD.getNumPartitions());
        //numbersRDD.foreach(i -> System.out.println(i));

        long startTime = System.currentTimeMillis();

        Double[] tt = GradientDescent.calculateGradient(sc, numbersRDD, 2, 731,0.1, 500, 0.001);

        long timeSpent = System.currentTimeMillis() - startTime;

        System.out.println("Метод выполнялся " + timeSpent + " миллисекунд");

        //Double[] res = GradientDescent.runWithSpark(sc, numbersRDD, 25, 2, 0.01, 10, 0.01);

        for (Double r:tt) {
            System.out.println(r);
        }
        System.out.println("Hello World");


    }
}
