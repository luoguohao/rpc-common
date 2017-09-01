package com.luogh.network.rpc.util;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author luogh
 */
@Slf4j
public class Util {

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            log.error("IOException should not have been thrown.", e);
        }
    }
}
