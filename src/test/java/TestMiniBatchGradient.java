package test.java;

import functions.LoadData;
import functions.MiniBatchGradientDescent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMiniBatchGradient {

    private JavaSparkContext sc;
    private JavaPairRDD<Double[], Double> numbersRDD;


    @Before
    public void setUp() {
        SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
        numbersRDD = LoadData.loadData(sc, "src/main/resources/sample.txt");
    }

    /**
     * @literal Тестовая выборка, а также ожидаемый результат
     * взяты из источника: https://crsmithdev.com/blog/ml-linear-regression/
     */
    @Test
    public void testFindParameters() {

        Double THETA_0_MIN  = 0.5;
        Double THETA_0_MAX = 2.5;

        Double THETA_1_MIN  = 5.0;
        Double THETA_1_MAX = 9.0;

        Double[] thetas = MiniBatchGradientDescent.findParameters(sc, numbersRDD,
                731, 2, 0.1, 500, 0);

        Assert.assertTrue(THETA_0_MIN < thetas[0] && thetas[0] < THETA_0_MAX);
        Assert.assertTrue(THETA_1_MIN < thetas[1] && thetas[1] < THETA_1_MAX);

    }
}
