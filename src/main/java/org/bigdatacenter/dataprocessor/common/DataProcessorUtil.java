package org.bigdatacenter.dataprocessor.common;

/**
 * Created by hyuk0 on 2017-06-21.
 */
public class DataProcessorUtil {
    public static boolean isNumeric(String value) {
        try {
            //noinspection ResultOfMethodCallIgnored
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
