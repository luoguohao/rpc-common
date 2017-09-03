package com.luogh.network.rpc.common;

import java.util.NoSuchElementException;

/**
 * @author luogh
 */
public class SystemPropertyConfigProvider extends ConfigProvider {

    @Override
    public String get(String key) {

        String value = System.getProperty(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return value;
    }
}
