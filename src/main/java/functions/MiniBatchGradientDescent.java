package functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;


public class MiniBatchGradientDescent {


    private static final Double INIT_THETAS_VALUE = 0.0;

    public static Double[] findParameters(JavaSparkContext jsc,
                                          JavaPairRDD<Double[], Double> data,
                                          long dataSize,
                                          int thetasAmount,
                                          double alpha,
                                          int maxIteration,
                                          double threshold) {

        Double error = Double.MAX_VALUE;
        Double[] thetas = initThetas(thetasAmount);
        Broadcast<Double[]> thetasVector = jsc.broadcast(thetas);

        int iteration = 0;
        while (iteration != maxIteration) {
            iteration++;

            thetasVector = jsc.broadcast(updateParameters(jsc, data, thetasVector,
                    dataSize, thetasAmount, alpha));
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

    private static Double[] initThetas(int thetasAmount) {
        Double[] thetas = new Double[thetasAmount];
        for (int j = 0; j < thetasAmount; j++) {
            thetas[j] = INIT_THETAS_VALUE;
        }
        return thetas;
    }

    private static Double[] updateParameters(JavaSparkContext jsc,
                                             JavaPairRDD<Double[], Double> data,
                                             Broadcast<Double[]> thetasVector,
                                             long dataSize,
                                             int thetasAmount,
                                             double alpha) {

        Double[] thetas = thetasVector.getValue();

        for (int j = 0; j < thetasAmount; j++) {
            Broadcast<Integer> currentJ = jsc.broadcast(j);
            DoubleAccumulator tempJAccumulator = jsc.sc().doubleAccumulator();
            tempJAccumulator.setValue(thetas[j]);

            data.foreach((currentData) -> {
                Double[] x = currentData._1();
                Double y = currentData._2();

                Double h = calculateH(thetasVector.getValue(), x, thetasAmount);
                tempJAccumulator.add(calculateTempPart(currentJ, h, x, y));

            });
            thetas[j] -=(alpha / dataSize) * tempJAccumulator.value();
        }
        return thetas;
    }

    private static Double costFunction(JavaSparkContext jsc,
                                       JavaPairRDD<Double[], Double> data,
                                       Broadcast<Double[]> thetasVector,
                                       long dataSize,
                                       int thetasAmount) {
        DoubleAccumulator sumError = jsc.sc().doubleAccumulator();

        data.foreach((currentData) -> {
            Double[] x = currentData._1();
            Double y = currentData._2();

            double gap = calculateH(thetasVector.getValue(), x, thetasAmount) - y;
            sumError.add(Math.pow(gap, 2));
        });

        Double newMeanError = (1.0 / (2 * dataSize)) * sumError.value();
        System.out.println("Error: " + newMeanError);

        return newMeanError;
    }

    private static Double calculateH(Double[] thetaTemp, Double[] x, int thetasAmount) {
        Double h = 0.0;

        for (int i = 0; i < thetasAmount; i++) {
            if (i == 0)
                h += thetaTemp[i];
            else
                h += thetaTemp[i] * x[i-1];
        }
        return h;
    }

    private static double calculateTempPart(Broadcast<Integer> currentJ,
                                            Double h, Double[] x, Double y) {
        double tempPart;

        int currJ = currentJ.getValue();
        if (currJ == 0)
            tempPart = (h - y);
        else
            tempPart = (h - y) * x[currJ - 1];

        return tempPart;
    }

}
