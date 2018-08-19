package functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public  class GradientDescent {


    public Double[] calculateGradient(JavaSparkContext jsc,
                                      JavaPairRDD<Double[], Double> data,
                                      int thetasAmount,
                                      int samplesSize,
                                      double alpha,
                                      double maxIteration,
                                      double threshold) {

        boolean exitFlag = false;
        int iteration = 0;

        // Инициализация параметров
        Double[] thetas = new Double[thetasAmount];
        for (int j = 0; j < thetasAmount; j++)
            thetas[j] = 1.0;

        Broadcast<Double[]> thetaWeights = jsc.broadcast(thetas);

        while (!exitFlag) {
            iteration++;
            if (iteration < maxIteration)
                exitFlag = true;

            // Для всех тета
            for (int j = 0; j < thetasAmount; j++) {

                Broadcast<Integer> currentJ = jsc.broadcast(j);
                DoubleAccumulator accumTempJ = jsc.sc().doubleAccumulator();


                data.foreach( (dataFrame) -> {

                        Double[] x = dataFrame._1();
                        Double y = dataFrame._2();
                        Double h = 0.0;
                        Double tempPart;

                        for (int i = 0; i < thetasAmount; i++) {
                            h += thetaWeights.getValue()[i] * x[i];
                        }
                        tempPart = (h - y)* x[currentJ.getValue()];
                        accumTempJ.add(tempPart);

                        });


                thetas[j] += alpha * (1.0 / samplesSize) * accumTempJ.value();

                for (int k = 0; k < thetasAmount; k++) {
                    System.out.println("theta_" + k + ": " + thetas[k]);
                }
            }




        }

        return new Double[] {0.0};
    }


}
