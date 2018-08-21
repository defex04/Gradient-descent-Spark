package functions;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesFromFile {

    public static String DATA_PATH;
    public static long DATA_SIZE;
    public static int THETAS_AMOUNT;
    static double INIT_THETAS_VALUE;
    public static double ALPHA;
    public static int MAX_ITERATION;
    public static double THRESHOLD;

    public static void setPropertiesFromFile(String path) throws IOException {

        FileInputStream fileInputStream;
        Properties configFile = new Properties();

        fileInputStream = new FileInputStream(path);
        configFile.load(fileInputStream);

        DATA_PATH = configFile.getProperty("dataPath");
        DATA_SIZE = Integer.parseInt(configFile.getProperty("dataSize"));
        THETAS_AMOUNT = Integer.parseInt(configFile.getProperty("thetasAmount"));
        INIT_THETAS_VALUE = Double.parseDouble(configFile.getProperty("initThetasValue"));
        ALPHA = Double.parseDouble(configFile.getProperty("alpha"));
        MAX_ITERATION = Integer.parseInt(configFile.getProperty("maxIteration"));
        THRESHOLD = Double.parseDouble(configFile.getProperty("threshold"));
    }

}
