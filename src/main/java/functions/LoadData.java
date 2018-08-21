package functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class LoadData {

    public static JavaPairRDD<Double[], Double> loadData(JavaSparkContext sc, String path) {

        JavaRDD<String> lines = sc.textFile(path);

        return lines.mapToPair(s -> {
                    String[] splitString = s.split(" ");
                    Double y = Double.parseDouble(splitString[splitString.length - 1]);
                    Double[] x = new Double[1];

                    for (int i =0; i < 1; i++) {
                        x[i] = Double.parseDouble(splitString[i]);
                    }
                    return new Tuple2<>(x, y);
                }
        );
    }
}
