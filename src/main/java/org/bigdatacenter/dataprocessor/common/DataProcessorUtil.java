package org.bigdatacenter.dataprocessor.common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by Anthony Jinhyuk Kim on 2017-06-21.
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

    public static String getHashedString(String string) {
        StringBuilder hashedStringBuilder = new StringBuilder();
        try {
            MessageDigest sh = MessageDigest.getInstance("SHA-256");
            sh.update(string.getBytes());

            for (byte aByteData : sh.digest())
                hashedStringBuilder.append(Integer.toString((aByteData & 0xff) + 0x100, 16).substring(1));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
        return hashedStringBuilder.toString();
    }
}
