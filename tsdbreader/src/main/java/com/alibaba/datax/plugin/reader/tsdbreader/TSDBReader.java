package com.alibaba.datax.plugin.reader.tsdbreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.HttpClientUtil;
import com.alibaba.datax.plugin.reader.tsdbreader.conn.DataPoint4MultiFieldsTSDB;
import com.alibaba.datax.plugin.reader.tsdbreader.conn.TSDBConnection;
import com.alibaba.datax.plugin.reader.tsdbreader.util.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.alibaba.datax.plugin.reader.tsdbreader.Key.JOB_BEGIN_DATE_TIME;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Reader
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
@SuppressWarnings("unused")
public class TSDBReader extends Reader {

    private static List<String> oidList;
    private static  HttpClientUtil httpClientUtil;
    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;
        private int startOidIndex;
        private int endOidIndex;
        private int oidTotalNums;
        private int oidUseNums;
        private int oidSpiteNums;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

            String type = originalConfig.getString(Key.SINK_DB_TYPE, Key.TYPE_DEFAULT_VALUE);
            if (StringUtils.isBlank(type)) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.SINK_DB_TYPE + "] is not set.");
            }
            if (!Key.TYPE_SET.contains(type)) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.SINK_DB_TYPE + "] should be one of [" +
                                JSON.toJSONString(Key.TYPE_SET) + "].");
            }

            String address = originalConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(address)) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.ENDPOINT + "] is not set.");
            } else if (!address.startsWith("http://")) {
                address = "http://" + address;
            }

            // tagK / field could be empty
            if ("TSDB".equals(type)) {
                List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw DataXException.asDataXException(
                            TSDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set.");
                }
            } else if ("RDB".equals(type)) {
                List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
                if (columns == null || columns.isEmpty()) {
                    throw DataXException.asDataXException(
                            TSDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.COLUMN + "] is not set.");
                }
                for (String specifyKey : Constant.MUST_CONTAINED_SPECIFY_KEYS) {
                    if (!columns.contains(specifyKey)) {
                        throw DataXException.asDataXException(
                                TSDBReaderErrorCode.ILLEGAL_VALUE,
                                "The parameter [" + Key.COLUMN + "] should contain "
                                        + JSON.toJSONString(Constant.MUST_CONTAINED_SPECIFY_KEYS) + ".");
                    }
                }
                final List<String> metrics = originalConfig.getList(Key.METRIC, String.class);
                if (metrics == null || metrics.isEmpty()) {
                    throw DataXException.asDataXException(
                            TSDBReaderErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.METRIC + "] is not set.");
                }
            } else if ("LINDORM-MIGRATION".equals(type)) {
                String oidPath = originalConfig.getString(Key.OID_PATH);
                Boolean oidCache = originalConfig.getBool(Key.OID_CACHE, true);
                int selfId = originalConfig.getInt(Key.SELF_ID);
                List jobIds = (List<Integer>)(originalConfig.getList(Key.JOB_IDS, Collections.EMPTY_LIST));
                List metrics = (List<String>)(originalConfig.getList(Key.METRICS, Collections.EMPTY_LIST));
                Collections.sort(jobIds);
                int jobIndex = jobIds.indexOf(selfId);

                // check oid file
                oidTotalNums = checkOidFile(oidPath, address, metrics);

                // assign self used oid
                if (oidTotalNums < jobIds.size()) {
                    throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE, "oid 数量小于任务数量");
                }
                oidUseNums = oidTotalNums / jobIds.size();
                if (oidUseNums < 10000) {
                    oidSpiteNums = oidUseNums;
                } else if (oidUseNums < 100000) {
                    oidSpiteNums = oidUseNums / 2;
                } else if (oidUseNums < 1000000) {
                    oidSpiteNums = oidUseNums / 10;
                } else if (oidUseNums < 10000000) {
                    oidSpiteNums = oidUseNums / 20;
                } else {
                    oidSpiteNums = oidUseNums / 30;
                }

                // cache self owned oid if needed
                startOidIndex = jobIndex * oidUseNums;
                endOidIndex = jobIndex >= jobIds.size() - 1 ? oidTotalNums : (jobIndex + 1) * oidUseNums;
                if (oidList == null && oidCache) {
                    oidList = new ArrayList();
                    try {
                        File oidFile = new File(oidPath);
                        LineIterator lineIterator = FileUtils.lineIterator(oidFile);
                        int index = 0;
                        while (lineIterator.hasNext()) {
                            if (index >= startOidIndex && index < endOidIndex) {
                                String oid = lineIterator.nextLine();
                                oidList.add(oid);
                                index++;
                            } else if (index >= endOidIndex) {
                                break;
                            } else {
                                lineIterator.nextLine();
                                index++;
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            Integer splitIntervalMs = originalConfig.getInt(Key.INTERVAL_DATE_TIME,
                    Key.INTERVAL_DATE_TIME_DEFAULT_VALUE);
            if (splitIntervalMs <= 0) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.INTERVAL_DATE_TIME + "] should be great than zero.");
            }

            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            String startTime = originalConfig.getString(Key.BEGIN_DATE_TIME);
            Long startDate;
            if (startTime == null || startTime.trim().length() == 0) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.BEGIN_DATE_TIME + "] is not set.");
            } else {
                try {
                    startDate = format.parse(startTime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.BEGIN_DATE_TIME +
                                    "] needs to conform to the [" + Constant.DEFAULT_DATA_FORMAT + "] format.");
                }
            }
            String endTime = originalConfig.getString(Key.END_DATE_TIME);
            Long endDate;
            if (endTime == null || endTime.trim().length() == 0) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.END_DATE_TIME + "] is not set.");
            } else {
                try {
                    endDate = format.parse(endTime).getTime();
                } catch (ParseException e) {
                    throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.END_DATE_TIME +
                                    "] needs to conform to the [" + Constant.DEFAULT_DATA_FORMAT + "] format.");
                }
            }
            if (startDate >= endDate) {
                throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE,
                        "The parameter [" + Key.BEGIN_DATE_TIME +
                                "] should be less than the parameter [" + Key.END_DATE_TIME + "].");
            }
        }

        private int checkOidFile(String oidPath, String address, List<String> metrics) {
            int oidNum;
            File oidFile = new File(oidPath);
            String startIndex = "";
            if (!oidFile.exists() || !oidFile.isFile() || oidFile.length() == 0) {
                // create oid file from mete query result
                oidNum = updateOidFile(oidPath, address, "", metrics);
            } else {
                oidNum = getFileLineNum(oidPath);
                try (ReversedLinesFileReader reversedLinesReader = new ReversedLinesFileReader(oidFile)) {
                    startIndex = reversedLinesReader.readLine();
                    oidNum += updateOidFile(oidPath, address, startIndex, metrics);
                } catch (Exception e) {
                    throw DataXException.asDataXException(TSDBReaderErrorCode.ILLEGAL_VALUE, "读oid文件出错", e);
                }
            }
            return oidNum;
        }

        private int getFileLineNum(String oidFile) {
            try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(oidFile))){
                lineNumberReader.skip(Long.MAX_VALUE);
                int lineNumber = lineNumberReader.getLineNumber();
                return lineNumber;
            } catch (IOException e) {
                return -1;
            }
        }

        private int updateOidFile(String oidFile, String address, String startIndex, List<String> metrics) {
            int lineNum = 0;
            httpClientUtil = new HttpClientUtil();
            try (FileWriter fw = new FileWriter(oidFile, true);
                BufferedWriter buf = new BufferedWriter(fw)) {
                URL url = new URL(address + "/api/search/tsuids");
                HttpPost httpPost = new HttpPost(url.toURI());
                while (true) {
                    Map content = new HashMap();
                    content.put("limit", 10000);
                    content.put("startIndex", startIndex);

                    Iterator<String> metricIter = metrics.iterator();
                    if (metricIter.hasNext()) {
                        StringBuilder metricsStrBuilder = new StringBuilder();
                        metricsStrBuilder.append(metricIter.next().trim());
                        while (metricIter.hasNext()) {
                            metricsStrBuilder.append("|").append(metricIter.next().trim());
                        }
                        content.put("metric", metricsStrBuilder.toString());
                    }

                    httpPost.setEntity(new StringEntity(JSON.toJSONString(content), "utf-8"));
                    String result = httpClientUtil.executeAndGetWithRetry(httpPost, 3, 10000);
                    JSONObject jsonResult = (JSONObject) JSON.parse(result);
                    if (jsonResult.getJSONArray("results").size() == 0) {
                        break;
                    }
                    Iterator iter = jsonResult.getJSONArray("results").iterator();
                    while (iter.hasNext()) {
                        startIndex = String.valueOf(iter.next());
                        buf.write(startIndex);
                        buf.newLine();
                        lineNum++;
                    }
                }

            } catch (IOException | URISyntaxException e) {
                e.printStackTrace();
            }
            return lineNum;
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<>();

            // get metrics
            String type = originalConfig.getString(Key.SINK_DB_TYPE, Key.TYPE_DEFAULT_VALUE);
            List<String> columns4TSDB = null;
            List<String> columns4RDB = null;
            List<String> metrics = null;
            if ("TSDB".equals(type)) {
                columns4TSDB = originalConfig.getList(Key.COLUMN, String.class);
            } else if ("RDB".equals(type)){
                columns4RDB = originalConfig.getList(Key.COLUMN, String.class);
                metrics = originalConfig.getList(Key.METRIC, String.class);
            }

            // get time interval
            Integer splitIntervalMs = originalConfig.getInt(Key.INTERVAL_DATE_TIME,
                    Key.INTERVAL_DATE_TIME_DEFAULT_VALUE);

            // get time range
            SimpleDateFormat format = new SimpleDateFormat(Constant.DEFAULT_DATA_FORMAT);
            long startTime;
            try {
                startTime = format.parse(originalConfig.getString(Key.BEGIN_DATE_TIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "解析[" + Key.BEGIN_DATE_TIME + "]失败.", e);
            }
            long endTime;
            try {
                endTime = format.parse(originalConfig.getString(Key.END_DATE_TIME)).getTime();
            } catch (ParseException e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "解析[" + Key.END_DATE_TIME + "]失败.", e);
            }
            if (TimeUtils.isSecond(startTime)) {
                startTime *= 1000;
            }
            if (TimeUtils.isSecond(endTime)) {
                endTime *= 1000;
            }
            if ("TSDB".equals(type)) {
                // split by metric
                for (String column : columns4TSDB) {
                    DateTime startDateTime = new DateTime(TimeUtils.getTimeInHour(startTime));
                    DateTime endDateTime = new DateTime(TimeUtils.getTimeInHour(endTime));

                    // split by time in hour
                    while (startDateTime.isBefore(endDateTime)) {
                        Configuration clone = this.originalConfig.clone();
                        clone.set(Key.COLUMN, Collections.singletonList(column));

                        clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                        startDateTime = startDateTime.plusMillis(splitIntervalMs);
                        // Make sure the time interval is [start, end).
                        clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                        clone.set("loadBalanceResourceMark", 0);
                        configurations.add(clone);

                        LOG.info("Configuration: {}", JSON.toJSONString(clone));
                    }
                }
            } else if ("RDB".equals(type)){
                // split by metric
                for (String metric : metrics) {
                    DateTime startDateTime = new DateTime(TimeUtils.getTimeInHour(startTime));
                    DateTime endDateTime = new DateTime(TimeUtils.getTimeInHour(endTime));

                    // split by time in hour
                    while (startDateTime.isBefore(endDateTime)) {
                        Configuration clone = this.originalConfig.clone();
                        clone.set(Key.COLUMN, columns4RDB);
                        clone.set(Key.METRIC, Collections.singletonList(metric));

                        clone.set(Key.BEGIN_DATE_TIME, startDateTime.getMillis());
                        startDateTime = startDateTime.plusMillis(splitIntervalMs);
                        // Make sure the time interval is [start, end).
                        clone.set(Key.END_DATE_TIME, startDateTime.getMillis() - 1);
                        clone.set("loadBalanceResourceMark", 0);
                        configurations.add(clone);

                        LOG.info("Configuration: {}", JSON.toJSONString(clone));
                    }
                }
            } else if ("LINDORM-MIGRATION".equals(type)) {
                String jobName = originalConfig.getString(Key.JOB_NAME);
                String oidPath = originalConfig.getString(Key.OID_PATH);
                int oidBatch = originalConfig.getInt(Key.OID_BATCH);
                int selfId = originalConfig.getInt(Key.SELF_ID);
                List jobIds = (List<Integer>)((List)(originalConfig.getList(Key.JOB_IDS)));
                DateTime startDateTime = new DateTime(TimeUtils.getTimeInHour(startTime));
                DateTime endDateTime = new DateTime(TimeUtils.getTimeInHour(endTime));
                int i = 0;
                long jobStart = startDateTime.getMillis();
                while (startDateTime.isBefore(endDateTime)) {
                    long start = startDateTime.getMillis();
                    long end = startDateTime.plusMillis(splitIntervalMs).getMillis();
                    for (int startOid = startOidIndex; startOid < endOidIndex; startOid += oidSpiteNums) {
                        // split by time in hour
                        int endOid = startOid + oidSpiteNums > endOidIndex ? endOidIndex : startOid + oidSpiteNums;

                        Configuration clone = this.originalConfig.clone();
                        clone.set(Key.OID_PATH, oidPath);
                        clone.set(Key.OID_CACHE, originalConfig.getBool(Key.OID_CACHE, false));
                        clone.set(Key.OID_START, startOid);
                        clone.set(Key.OID_END, endOid);
                        clone.set(Key.BEGIN_DATE_TIME, start);
                        clone.set(Key. JOB_BEGIN_DATE_TIME, jobStart);
                        clone.set(Key.OID_BATCH, oidBatch);
                        clone.set(Key.JOB_NAME, jobName);
                        clone.set(Key.SELF_ID, selfId);
                        clone.set(Key.JOB_IDS, jobIds);
                        // Make sure the time interval is [start, end).
                        clone.set(Key.END_DATE_TIME, end - 1);
                        clone.set("loadBalanceResourceMark", 0);
                        configurations.add(clone);
                        LOG.info("Configuration: {}", JSON.toJSONString(clone));
                    }
                    startDateTime = startDateTime.plusMillis(splitIntervalMs);
                }
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        public int getLineNumber(String path) {
            try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(path))){
                lineNumberReader.skip(Long.MAX_VALUE);
                int lineNumber = lineNumberReader.getLineNumber();
                return lineNumber;
            } catch (IOException io) {
                return -1;
            }
        }
    }

    public static class Task extends Reader.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private String type;
        private List<String> columns4TSDB = null;
        private List<String> columns4RDB = null;
        private List<String> metrics = null;
        private Map<String, Object> fields;
        private Map<String, Object> tags;
        private TSDBConnection conn;
        private Long startTime;
        private Long endTime;
        // for LINDORM-MIGRATION
        private String internalMetricPrefix = "internal_datax_job";
        private String jobName;
        private Long jobStartTime;
        private int selfId;
        private List jobIds;
        private int startOid;
        private int endOid;
        private int oidBatch;
        private String oidPath;
        Boolean oid_cache;

        @Override
        public void init() {
            Configuration readerSliceConfig = super.getPluginJobConf();

            LOG.info("getPluginJobConf: {}", JSON.toJSONString(readerSliceConfig));

            this.type = readerSliceConfig.getString(Key.SINK_DB_TYPE);
            if ("TSDB".equals(type)) {
                columns4TSDB = readerSliceConfig.getList(Key.COLUMN, String.class);
                this.fields = readerSliceConfig.getMap(Key.FIELD);
                this.tags = readerSliceConfig.getMap(Key.TAG);
            } else if ("RDS".equals(type)) {
                columns4RDB = readerSliceConfig.getList(Key.COLUMN, String.class);
                metrics = readerSliceConfig.getList(Key.METRIC, String.class);
                this.fields = readerSliceConfig.getMap(Key.FIELD);
                this.tags = readerSliceConfig.getMap(Key.TAG);
            } else if ("LINDORM-MIGRATION".equals(type)) {
                jobName = readerSliceConfig.getString(Key.JOB_NAME);
                startOid = readerSliceConfig.getInt(Key.OID_START);
                endOid = readerSliceConfig.getInt(Key.OID_END);
                oidBatch = readerSliceConfig.getInt(Key.OID_BATCH);
                oidPath = readerSliceConfig.getString(Key.OID_PATH);
                oid_cache = readerSliceConfig.getBool(Key.OID_CACHE, false);
                selfId = readerSliceConfig.getInt(Key.SELF_ID);
                jobIds = (List<Integer>)((List)(readerSliceConfig.getList(Key.JOB_IDS)));
                jobStartTime = readerSliceConfig.getLong(JOB_BEGIN_DATE_TIME);
            }


            String address = readerSliceConfig.getString(Key.ENDPOINT);

            conn = new TSDBConnection(address);

            this.startTime = readerSliceConfig.getLong(Key.BEGIN_DATE_TIME);
            this.endTime = readerSliceConfig.getLong(Key.END_DATE_TIME);
        }

        @Override
        public void prepare() {
        }

        @Override
        @SuppressWarnings("unchecked")
        public void startRead(RecordSender recordSender) {
            try {
                if ("TSDB".equals(type)) {
                    for (String metric : columns4TSDB) {
                        final Map<String, String> tags = this.tags == null ?
                                    null : (Map<String, String>) this.tags.get(metric);
                        if (fields == null || !fields.containsKey(metric)) {
                            conn.sendDPs(metric, tags, this.startTime, this.endTime, recordSender);
                        } else {
                            conn.sendDPs(metric, (List<String>) fields.get(metric),
                                    tags, this.startTime, this.endTime, recordSender);
                        }
                    }
                } else if ("RDS".equals(type)) {
                    for (String metric : metrics) {
                        final Map<String, String> tags = this.tags == null ?
                                null : (Map<String, String>) this.tags.get(metric);
                        if (fields == null || !fields.containsKey(metric)) {
                            conn.sendRecords(metric, tags, startTime, endTime, columns4RDB, recordSender);
                        } else {
                            conn.sendRecords(metric, (List<String>) fields.get(metric),
                                    tags, startTime, endTime, columns4RDB, recordSender);
                        }
                    }
                } else if ("LINDORM-MIGRATION".equals(type)) {
                    // wait while previous time duration finished
                    while (true) {
                        if (jobStartTime.equals(startTime) || checkPreviousJobFinished(startTime, jobIds, jobName)) {
                            break;
                        }
                        Thread.sleep(3000L);
                        LOG.info("wait for previous job :" + startTime + " finished");
                    }

                    LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(oidPath));

                    if (!oid_cache) {
                        String oid = lineNumberReader.readLine();
                        List<String> oids = new ArrayList();
                        int subI = 0;
                        while (oid != null) {
                            if (lineNumberReader.getLineNumber() < startOid) {
                                oid = lineNumberReader.readLine();
                            } else if (lineNumberReader.getLineNumber() >= startOid && lineNumberReader.getLineNumber() < endOid) {
                                oids.add(oid);
                                subI++;
                                if (oids.size() >= oidBatch) {
                                    LOG.info("startTime: {}, endTime: {}, startOid: {}, endOid: {}, subStart:{}, subEnd{}",
                                            startTime, endTime, startOid, endOid, subI - oids.size(), subI);
                                    conn.sendRecords(new ArrayList<>(oids), startTime, endTime, recordSender);
                                    oids.clear();
                                }
                                oid = lineNumberReader.readLine();
                            } else {
                                break;
                            }
                        }
                        if (!oids.isEmpty()) {
                            conn.sendRecords(new ArrayList<>(oids), startTime, endTime, recordSender);
                        }
                    } else {
                        for (int i = 0; i < oidList.size(); i += oidBatch) {
                            int tempEndOid = i + oidBatch;
                            if (tempEndOid > oidList.size()) {
                                tempEndOid = oidList.size();
                            }
                            LOG.info("startTime: {}, endTime: {}, startOid: {}, endOid: {}, subStart:{}, subEnd{}",
                                    startTime, endTime, startOid, endOid, i, tempEndOid);
                            conn.sendRecords(oidList.subList(i, tempEndOid), startTime, endTime, recordSender);
                        }
                    }
                    updateJobStats(endTime, jobName, selfId);
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "获取或发送数据点的过程中出错！", e);
            }
        }

        private boolean checkPreviousJobFinished(Long startTime, List jobIds, String jobName) {
            try {
                String result = conn.queryRange4MultiFields(internalMetricPrefix + jobName, Collections.singletonList("state"),
                        Collections.emptyMap(), startTime - 1, startTime);
                return JSON.parseArray(result).size() == jobIds.size();
            } catch (Exception e) {
                throw DataXException.asDataXException(
                        TSDBReaderErrorCode.ILLEGAL_VALUE, "检查前置任务 " + startTime + " 状态的过程中出错！", e);
            }
        }

        private void updateJobStats(Long endTime, String jobName, int selfId) {
            Map tag = new HashMap() {{put("job", String.valueOf(selfId));}};
            Map field = new HashMap() {{put("state", "OK");}};
            DataPoint4MultiFieldsTSDB dataPoint4MultiFieldsTSDB = new DataPoint4MultiFieldsTSDB(endTime, internalMetricPrefix + jobName,
                    tag, field);
            Boolean succ = conn.mput(dataPoint4MultiFieldsTSDB);
            if (!succ) {
                throw new RuntimeException("同步任务状态 job:" + selfId + ":" + endTime);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
