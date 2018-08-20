package functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;


public class MiniBatchGradientDescent {


    public static Double[] findParameters(JavaSparkContext jsc,
                                          JavaPairRDD<Double[], Double> data,
                                          long dataSize,
                                          int thetasAmount,
                                          double alpha,
                                          int maxIteration,
                                          double threshold) {

        Double error = Double.MAX_VALUE;
        Double INIT_THETAS_VALUE = 0.0;

        // Инициализация параметров theta
        Double[] thetas = new Double[thetasAmount];
        for (int j = 0; j < thetasAmount; j++) {
            thetas[j] = INIT_THETAS_VALUE;
        }


        Broadcast<Double[]> thetasVector = jsc.broadcast(thetas);

        int iteration = 0;

        while (true) {
            iteration++;
            if (iteration > maxIteration)
                break;


            Double[] newThetas = updateParameters(jsc, data, thetasVector, dataSize, thetasAmount, alpha);

            thetasVector = jsc.broadcast(newThetas);

            Double newError = costFunction(jsc, data, thetasVector, dataSize, thetasAmount);

            if (Math.abs(error-newError) < threshold) {
                System.out.println("Number of iterations: " + iteration);
                break;
            }
            else
                error = newError;

        }

        return thetas;

    }


    private static Double[] updateParameters(JavaSparkContext jsc,
                                             JavaPairRDD<Double[], Double> data,
                                             Broadcast<Double[]> thetasVector,
                                             long dataSize,
                                             int thetasAmount,
                                             double alpha) {
        // Для совместного использования при вычислении новых значений theta
        Double[] thetas = thetasVector.getValue();

        for (int j = 0; j < thetasAmount; j++) {
            Broadcast<Integer> currentJ = jsc.broadcast(j);
            DoubleAccumulator accumTempJ = jsc.sc().doubleAccumulator();
            accumTempJ.setValue(thetas[j]);

            data.foreach( (currentData) -> {
                Double[] x = currentData._1();
                Double y = currentData._2();
                double tempPart;

                Double[] thetaTemp = thetasVector.getValue(); //
                Double h = 0.0;

                for (int i = 0; i < thetasAmount; i++) {
                    if (i == 0)
                        h += thetaTemp[i];
                    else
                        h += thetaTemp[i] * x[i-1];
                }

                int currJ = currentJ.getValue();
                if (currJ == 0)
                    tempPart = (h - y); //
                else
                    tempPart = (h - y) * x[currJ - 1]; //
                accumTempJ.add(tempPart);

            } );

            thetas[j] -=(alpha / dataSize) * accumTempJ.value();

        }

        for (int i = 0; i < thetasAmount; i++) {
            System.out.println("theta_" + i + ": " + thetas[i]);
        }

        return thetas;
    }


    private static Double costFunction(JavaSparkContext jsc,
                                       JavaPairRDD<Double[], Double> data,
                                       Broadcast<Double[]> thetasVector,
                                       long dataSize,
                                       int thetasAmount) {
        DoubleAccumulator sumError = jsc.sc().doubleAccumulator();

        data.foreach( (currentData) -> {
            Double[] x = currentData._1();
            Double y = currentData._2();

            Double[] thetaTemp = thetasVector.getValue();
            double h = 0.0;

            for (int i = 0; i < thetasAmount; i++) {
                if (i == 0)
                    h += thetaTemp[i];
                else
                    h += thetaTemp[i] * x[i-1];
            }

            double gap = h - y;
            sumError.add(Math.pow(gap, 2));

        });

        Double newMeanError = (1.0 / (2 * dataSize)) * sumError.value();
        System.out.println("Error: " + newMeanError);

        return newMeanError;
    }



}
