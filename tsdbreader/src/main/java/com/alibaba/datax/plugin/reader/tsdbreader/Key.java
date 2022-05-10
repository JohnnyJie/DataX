package com.alibaba.datax.plugin.reader.tsdbreader;

import java.util.HashSet;
import java.util.Set;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Key
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public class Key {

    // TSDB for OpenTSDB / InfluxDB / TimeScale / Prometheus etc.
    // RDB for MySQL / ADB etc.
    static final String SINK_DB_TYPE = "sinkDbType";
    static final String ENDPOINT = "endpoint";
    static final String COLUMN = "column";
    static final String METRIC = "metric";
    static final String FIELD = "field";
    static final String TAG = "tag";
    static final String INTERVAL_DATE_TIME = "splitIntervalMs";
    static final String BEGIN_DATE_TIME = "beginDateTime";
    static final String END_DATE_TIME = "endDateTime";

    static final Integer INTERVAL_DATE_TIME_DEFAULT_VALUE = 60000;
    static final String TYPE_DEFAULT_VALUE = "TSDB";
    static final Set<String> TYPE_SET = new HashSet<>();

    static {
        TYPE_SET.add("TSDB");
        TYPE_SET.add("RDB");
        TYPE_SET.add("LINDORM-MIGRATION");
    }

    /**
     *  For LINDORM-MIGRATION
     */
    static final String OID_PATH = "oidPath";
    static final String OID_BATCH = "oidBatch";
    static final String OID_START = "startOid";
    static final String OID_END = "endOid";
    static final String OID_INTERVAL = "oidInterval";
    static final String OID_CACHE ="oidCache";
    static final String JOB_NAME = "jobName";
    static final String SELF_ID = "selfId";
    static final String JOB_IDS = "jobIds";
    static final String JOB_BEGIN_DATE_TIME = "jobBeginDateTime";
}
