package com.alibaba.datax.plugin.reader.tsdbreader.util;

import com.alibaba.datax.plugin.reader.tsdbreader.conn.DataPoint4TSDB;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Utils
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public final class TSDBUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TSDBUtils.class);

    private TSDBUtils() {
    }

    public static String version(String address) {
        String url = String.format("%s/api/version", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static String config(String address) {
        String url = String.format("%s/api/config", address);
        String rsp;
        try {
            rsp = HttpUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rsp;
    }

    public static boolean put(String address, List<DataPoint4TSDB> dps) {
        return put(address, JSON.toJSON(dps));
    }

    public static boolean put(String address, DataPoint4TSDB dp) {
        return put(address, JSON.toJSON(dp));
    }

    private static boolean put(String address, Object o) {
        String url = String.format("%s/api/put", address);
        String rsp;
        try {
            rsp = HttpUtils.post(url, o.toString());
            // If successful, the returned content should be null.
            assert rsp == null;
        } catch (Exception e) {
            LOGGER.error("Address: {}, DataPoints: {}", url, o);
            throw new RuntimeException(e);
        }
        return true;
    }

    public static String tsuids(String address, long start, long end, List<String> oids) {
        String url = String.format("%s/api/query/tsuid", address);
        String rsp;
        try {
            Map content = new HashMap();
            content.put("start", start);
            content.put("end", end);
            content.put("tsuids", oids);
            rsp = HttpUtils.post(url, content);
            return rsp;
        } catch (Exception e) {
            LOGGER.error("Address: {}, Query: {}", url, oids);
            throw new RuntimeException(e);
        }
    }

    public static boolean mput(String address, Object o) {
        String url = String.format("%s/api/mput", address);
        String rsp;
        try {
            rsp = HttpUtils.post(url, o.toString());
            // If successful, the returned content should be null.
            assert rsp == null;
        } catch (Exception e) {
            LOGGER.error("Address: {}, DataPoints: {}", url, o);
            throw new RuntimeException(e);
        }
        return true;
    }
}
