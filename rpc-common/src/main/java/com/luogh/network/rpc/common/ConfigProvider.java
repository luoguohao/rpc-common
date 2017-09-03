package com.luogh.network.rpc.common;

import java.util.NoSuchElementException;

/**
 * @author luogh
 */
public abstract class ConfigProvider {

    public abstract String get(String key);

    public String get(String key, String defValue) {
         try {
             return get(key);
         } catch (NoSuchElementException e) {
             return defValue;
         }
    }

    public int getInt(String key, int defValue) {
        return Integer.parseInt(get(key, Integer.toString(defValue)));
    }

    public long getLong(String key, long defValue) {
        return Long.parseLong(get(key, Long.toString(defValue)));
    }

    public double getDouble(String key, double defValue) {
        return Double.parseDouble(get(key, Double.toString(defValue)));
    }

    public boolean getBoolean(String key, boolean defValue) {
        return Boolean.getBoolean(get(key, Boolean.toString(defValue)));
    }
}
