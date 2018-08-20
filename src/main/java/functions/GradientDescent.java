package functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.DoubleAccumulator;


public  class GradientDescent {


    public static Double[] calculateGradient(JavaSparkContext jsc,
                                      JavaPairRDD<Double[], Double> data,
                                      int thetasAmount,
                                      int samplesSize,
                                      double alpha,
                                      double maxIteration,
                                      double threshold) {

        int iteration = 0;
        Double error = 0.0;

        // Инициализация параметров Theta начальными значениями
        Double[] thetas = new Double[thetasAmount];
        for (int j = 0; j < thetasAmount; j++)
            thetas[j] = 0.0;

        // Для совместного использования по всей программе
        Broadcast<Double[]> thetaWeights = jsc.broadcast(thetas);

        while (true) {
            iteration++;
            if (iteration > maxIteration)
                break;
            // Для совместного использования при вычислении новых значений theta
            Broadcast<Double[]> thetaWeights_1 = thetaWeights;

            // Update thetas
            for (int j = 0; j < thetasAmount; j++) {

                Broadcast<Integer> currentJ = jsc.broadcast(j);
                DoubleAccumulator accumTempJ = jsc.sc().doubleAccumulator();
                accumTempJ.setValue(thetas[j]);  //


                data.foreach( (dataFrame) -> {

                        Double[] x = dataFrame._1();
                        Double y = dataFrame._2();
                        int xSize = x.length;
                        Double tempPart;

                        Double[] theta_node = thetaWeights_1.getValue(); //
                        Double h = theta_node[thetasAmount-1]; //


                    for (int i = 0; i < thetasAmount-1; i++) {
                            h += theta_node[i] * x[i];
                        }

                        if (currentJ.getValue() == 0)
                            tempPart = (h - y); //
                        else
                            tempPart = (h - y)* x[0]; //
                        accumTempJ.add(tempPart);

                        });


                thetas[j] -= alpha * (1.0 / samplesSize) * accumTempJ.value();

                for (int k = 0; k < thetasAmount; k++) {
                    System.out.println("theta_" + k + ": " + thetas[k]);
                }

            }

            thetaWeights = jsc.broadcast(thetas);

            Broadcast<Double[]> thetaWeights_2 = thetaWeights; //

            // Оценка ошибки

            DoubleAccumulator sumError = jsc.sc().doubleAccumulator();


            // Для каждого набора данных
            data.foreach( (dataFrame) -> {
                Double[] x = dataFrame._1();
                Double y = dataFrame._2();
                int xSize = x.length;

                Double[] theta_node = thetaWeights_2.getValue();
                double h = 0.0;
                        //theta_node[thetasAmount-1] - y;


                for (int i = 0; i < thetasAmount-1; i++) {
                    h += theta_node[i] * x[i];
                }

                double gap = y - h;
                sumError.add(Math.pow(gap, 2));

                //sumError.add(h * h);
            });
            Double newMeanError = (1.0 / (2*samplesSize)) * sumError.value();

            //Double newMeanError = Math.sqrt(sumError.value());

            System.out.println(newMeanError);
            // Сравнение значений

            if (Math.abs(error - newMeanError) < threshold) {
                System.out.println("Number of iterations: " + iteration);
                break;
            }
            else {
                error = newMeanError;
            }


        }

        return thetas;
    }

}
