import functions.GradientDescent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;


public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\andre\\Desktop\\Gradient-descent-Spark\\src\\main\\java\\sample.txt");

        JavaPairRDD<Double[], Double> numbersRDD = lines.mapToPair(
                s -> {
                    String[] splitString = s.split(" ");
                    Double y = Double.parseDouble(splitString[splitString.length - 1]);
                    Double[] x = new Double[2];

                    for (int i =0; i < 2; i++) {
                        x[i] = Double.parseDouble(splitString[i]);
                    }
                    return new Tuple2<>(x, y);
                }
        );

        numbersRDD.persist(StorageLevel.MEMORY_ONLY());
        numbersRDD.collect().forEach(tuple -> { for(Double d : tuple._1()) System.out.print(d + " "); System.out.println(tuple._2()); });


        System.out.println(numbersRDD.getNumPartitions());
        numbersRDD.foreach(i -> System.out.println(i));

        //CustumFunction custumFunction = new CustumFunction();
        //custumFunction.applyThetas(new Double[] {1.0, 1.0});

        GradientDescent gradientDescent = new GradientDescent();
        Double[] tt = gradientDescent.calculateGradient(sc, numbersRDD, 2, 4,0.01, 25, 0.01);

        System.out.println("Hello World");


    }
}
