package com.luogh.network.rpc.common;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author luogh
 */
public class MapConfigProvider extends ConfigProvider {

    private final Map<String, String> properties;

    public MapConfigProvider(Map<String, String> properties) {
        this.properties = Maps.newHashMap(properties);
    }

    public MapConfigProvider() {
        this.properties = Maps.newHashMap();
    }

    @Override
    public String get(String key) {
        String value = properties.get(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return value;
    }
}
