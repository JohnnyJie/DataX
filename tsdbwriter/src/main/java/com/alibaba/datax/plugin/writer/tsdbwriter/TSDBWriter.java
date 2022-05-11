package com.alibaba.datax.plugin.writer.tsdbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.fastjson.JSON;
import com.aliyun.hitsdb.client.TSDB;
import com.aliyun.hitsdb.client.TSDBClientFactory;
import com.aliyun.hitsdb.client.TSDBConfig;
import com.aliyun.hitsdb.client.callback.BatchPutIgnoreErrorsCallback;
import com.aliyun.hitsdb.client.callback.MultiFieldBatchPutIgnoreErrorsCallback;
import com.aliyun.hitsdb.client.value.request.MultiFieldPoint;
import com.aliyun.hitsdb.client.value.request.Point;
import com.aliyun.hitsdb.client.value.response.batch.DetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.ErrorPoint;
import com.aliyun.hitsdb.client.value.response.batch.IgnoreErrorsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldDetailsResult;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldErrorPoint;
import com.aliyun.hitsdb.client.value.response.batch.MultiFieldIgnoreErrorsResult;
import com.aliyun.hitsdb.client.value.response.batch.SummaryResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Http Writer
 *
 * @author Benedict Jin
 * @since 2019-04-18
 */
@SuppressWarnings("unused")
public class TSDBWriter extends Writer {

    private static SourceDBType DB_TYPE;
    private static TSDB tsdb = null;

    public static class Job extends Writer.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            originalConfig = super.getPluginJobConf();

            // check source db type
            String sourceDbType = originalConfig.getString(Key.SOURCE_DB_TYPE);
            if (StringUtils.isBlank(sourceDbType)) {
                sourceDbType = SourceDBType.TSDB.name();
                originalConfig.set(Key.SOURCE_DB_TYPE, sourceDbType);
                LOG.info("The parameter [" + Key.SOURCE_DB_TYPE + "] will be default value: " + SourceDBType.TSDB);
            }
            try {
                DB_TYPE = SourceDBType.valueOf(sourceDbType);
            } catch (Exception e) {
                throw DataXException.asDataXException(TSDBWriterErrorCode.REQUIRED_VALUE,
                        "The parameter [" + Key.SOURCE_DB_TYPE +
                                "] is invalid, which should be one of [" + Arrays.toString(SourceDBType.values()) + "].");
            }

            originalConfig.getNecessaryValue(Key.ENDPOINT, TSDBWriterErrorCode.REQUIRED_VALUE);
            String endpoint = originalConfig.getString(Key.ENDPOINT);
            String ip;
            int port;
            if (endpoint.startsWith("http")) {
                String[] split = endpoint.split(":");

                if (split.length != 3) {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.ENDPOINT + "] is invalid, which should be [http://IP:Port] or [IP:Port].");
                }
                ip = split[1].substring(2);
                port = Integer.parseInt(split[2]);
            } else {
                String[] split = endpoint.split(":");
                if (split.length != 2) {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.REQUIRED_VALUE,
                            "The parameter [" + Key.ENDPOINT + "] is invalid, which should be [http://IP:Port] or [IP:Port].");
                }
                ip = split[0];
                port = Integer.parseInt(split[1]);
            }

            String username = originalConfig.getString(Key.USERNAME, null);
            if (StringUtils.isBlank(username)) {
                LOG.warn("The parameter [" + Key.USERNAME + "] is blank.");
            }

            String password = originalConfig.getString(Key.PASSWORD, null);
            if (StringUtils.isBlank(password)) {
                LOG.warn("The parameter [" + Key.PASSWORD + "] is blank.");
            }

            TSDBConfig.Builder builder = TSDBConfig.address(ip, port);


            if (!StringUtils.isBlank(password) && !StringUtils.isBlank(username)) {
                builder.basicAuth(username, password);
            }

            Integer batchSize = originalConfig.getInt(Key.BATCH_SIZE);
            if (batchSize != null && batchSize > 0) {
                builder.batchPutSize(batchSize);
            }

            Integer retrySize = originalConfig.getInt(Key.MAX_RETRY_TIME, Constant.DEFAULT_TRY_SIZE);
            if (retrySize != null && retrySize >= 0) {
                builder.batchPutRetryCount(retrySize);
            }

            Integer httpConnectionPool = originalConfig.getInt(Key.HTTP_CONNECTION_POOL);
            if (httpConnectionPool != null && httpConnectionPool > 0) {
                builder.httpConnectionPool(httpConnectionPool);
            }

            Boolean ignoreWriteError = originalConfig.getBool(Key.IGNORE_WRITE_ERROR, false);

            BatchPutIgnoreErrorsCallback batchPutIgnoreErrorsCallback = new BatchPutIgnoreErrorsCallback() {
                @Override
                public void response(String s, List<Point> list, IgnoreErrorsResult ignoreErrorsResult) {
                    if (ignoreErrorsResult == null) {
                        return;
                    }

                    List<ErrorPoint> ignoredErrors = ignoreErrorsResult.getIgnoredErrors();
                    if (ignoredErrors== null || ignoredErrors.isEmpty()) {
                        return;
                    }

                    LOG.error("Ignore write exceptions and continue writing for " + ignoreErrorsResult.getFailed() + "points.");
                }

                @Override
                public void failed(String address, List<Point> points, Exception ex) {
                    LOG.error("Ignore write exceptions and continue writing for " + points.size() + "points.", ex);
                }
            };
            builder.listenBatchPut(batchPutIgnoreErrorsCallback);

            MultiFieldBatchPutIgnoreErrorsCallback multiFieldBatchPutIgnoreErrorsCallback = new MultiFieldBatchPutIgnoreErrorsCallback() {
                @Override
                public void response(String s, List<MultiFieldPoint> list, MultiFieldIgnoreErrorsResult multiFieldIgnoreErrorsResult) {
                    if (multiFieldIgnoreErrorsResult == null) {
                        return;
                    }

                    List<MultiFieldErrorPoint> ignoredErrors = multiFieldIgnoreErrorsResult.getIgnoredErrors();
                    if (ignoredErrors== null || ignoredErrors.isEmpty()) {
                        return;
                    }
                    LOG.error("Ignore write exceptions and continue writing for " + multiFieldIgnoreErrorsResult.getFailed() + "points.");
                }

                @Override
                public void failed(String address, List<MultiFieldPoint> points, Exception ex) {
                    LOG.error("Ignore write exceptions and continue writing for " + points.size() + "points.", ex);
                }
            };
            builder.listenMultiFieldBatchPut(multiFieldBatchPutIgnoreErrorsCallback);

            tsdb = TSDBClientFactory.connect(builder.config());

            String database = originalConfig.getString(Key.DATABASE, null);
            if (StringUtils.isBlank(database)) {
                LOG.info("The parameter [" + Key.DATABASE + "] is blank.");
            } else {
                tsdb.useDatabase(database);
            }


            LOG.info("Tsdb config:" + originalConfig.toJSON());

            // for tsdb
            if (DB_TYPE == SourceDBType.RDB) {
                // for rdb
                originalConfig.getNecessaryValue(Key.COLUMN_TYPE, TSDBWriterErrorCode.REQUIRED_VALUE);
                originalConfig.getNecessaryValue(Key.COLUMN, TSDBWriterErrorCode.REQUIRED_VALUE);

            }
        }



        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            ArrayList<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                originalConfig.set("loadBalanceResourceMark", 0);
                configurations.add(originalConfig.clone());
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private boolean multiField;
        private int batchSize;
        private int retrySize;
        private boolean ignoreWriteError;
        private String tableName;
        private TSDBConverter tsdbConverter;

        @Override
        public void init() {
            Configuration writerSliceConfig = getPluginJobConf();

            // single field | multi fields
            this.multiField = writerSliceConfig.getBool(Key.MULTI_FIELD, false);
            this.retrySize = writerSliceConfig.getInt(Key.MAX_RETRY_TIME, Constant.DEFAULT_TRY_SIZE);
            this.ignoreWriteError = writerSliceConfig.getBool(Key.IGNORE_WRITE_ERROR, false);

            // for tsdb
            if (DB_TYPE == SourceDBType.TSDB) {
                String address = writerSliceConfig.getString(Key.ENDPOINT);
                String database = writerSliceConfig.getString(Key.DATABASE);
                String username = writerSliceConfig.getString(Key.USERNAME);
                String password = writerSliceConfig.getString(Key.PASSWORD);
                batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, 100);

            } else if (DB_TYPE == SourceDBType.RDB) {
                // for rdb
                int timeSize = 0;
                int fieldSize = 0;
                int tagSize = 0;

                batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE, 100);
                List<String> columnName = writerSliceConfig.getList(Key.COLUMN, String.class);
                List<String> columnType = writerSliceConfig.getList(Key.COLUMN_TYPE, String.class);
                Set<String> typeSet = new HashSet<String>(columnType);
                if (columnName.size() != columnType.size()) {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.COLUMN_TYPE + "] should has same length with [" + Key.COLUMN + "].");
                }

                for (String type : columnType) {
                    if (TSDBModel.TSDB_TAG.equals(type)) {
                        tagSize ++;
                    } else if (TSDBModel.TSDB_FIELD_DOUBLE.equals(type) || TSDBModel.TSDB_FIELD_STRING.equals(type)
                            || TSDBModel.TSDB_FIELD_BOOL.equals(type)) {
                        fieldSize++;
                    } else if (TSDBModel.TSDB_TIMESTAMP.equals(type)) {
                        timeSize++;
                    }
                }

                if (fieldSize == 0) {
                    // compatible with previous usage of TSDB_METRIC_NUM and TSDB_METRIC_STRING
                    if (!typeSet.contains(TSDBModel.TSDB_METRIC_NUM) && !typeSet.contains(TSDBModel.TSDB_METRIC_STRING)) {
                        throw DataXException.asDataXException(TSDBWriterErrorCode.ILLEGAL_VALUE,
                                "The parameter [" + Key.COLUMN_TYPE + "] is invalid, must set at least one of "
                                        + TSDBModel.TSDB_FIELD_DOUBLE + ", " + TSDBModel.TSDB_FIELD_STRING + " or " + TSDBModel.TSDB_FIELD_BOOL + ".");
                    }
                }

                if (tagSize == 0) {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.COLUMN_TYPE + "] is invalid, must set " + TSDBModel.TSDB_TAG + ". ");
                }

                if (timeSize != 1) {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.ILLEGAL_VALUE,
                            "The parameter [" + Key.COLUMN_TYPE + "] is invalid, must set one and only one "
                                    + TSDBModel.TSDB_TIMESTAMP + ".");
                }

                if (multiField) {
                    // check source db type
                    tableName = writerSliceConfig.getString(Key.TABLE);
                    if (StringUtils.isBlank(tableName)) {
                        throw DataXException.asDataXException(TSDBWriterErrorCode.ILLEGAL_VALUE,
                                "The parameter [" + Key.TABLE + "] h must set when use multi field input.");
                    }
                }
                tsdbConverter = new TSDBConverter(columnName, columnType);
            }

        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            // for tsdb
            if (DB_TYPE == SourceDBType.TSDB) {
                try {
                    Record lastRecord = null;
                    Record record;
                    List points = new ArrayList();
                    while ((record = recordReceiver.getFromReader()) != null) {
                        final int recordLength = record.getColumnNumber();
                        for (int i = 0; i < recordLength; i++) {
                            if (multiField) {
                                String recordRaw = record.getColumn(i).asString();
                                if (recordRaw.startsWith("[")) {
                                    List<MultiFieldPoint> point = JSON.parseArray(recordRaw, MultiFieldPoint.class);
                                    points.addAll(point);
                                } else {
                                    MultiFieldPoint point = JSON.parseObject(recordRaw, MultiFieldPoint.class);
                                    points.add(point);
                                }
                            } else {
                                String recordRaw = record.getColumn(i).asString();
                                if (recordRaw.startsWith("[")) {
                                    List<Point> point = JSON.parseArray(recordRaw, Point.class);
                                    points.addAll(point);
                                } else {
                                    Point point = JSON.parseObject(recordRaw, Point.class);
                                    points.add(point);
                                }
                            }

                            if (points.size() >= batchSize) {
                                batchPut(record, points);
                                points.clear();
                            }
                        }
                        lastRecord = record;
                    }
                    if (!points.isEmpty()) {
                        batchPut(lastRecord, points);
                    }
                } catch (Exception e) {
                    throw DataXException.asDataXException(TSDBWriterErrorCode.RUNTIME_EXCEPTION, e);
                }
            } else if (DB_TYPE == SourceDBType.RDB) {
                // for rdb
                List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
                Record record;
                long total = 0;
                while ((record = recordReceiver.getFromReader()) != null) {
                    writerBuffer.add(record);
                    if (writerBuffer.size() >= this.batchSize) {
                        total += doBatchInsert(writerBuffer);
                        writerBuffer.clear();
                    }
                }
                if (!writerBuffer.isEmpty()) {
                    total += doBatchInsert(writerBuffer);
                    writerBuffer.clear();
                }
                getTaskPluginCollector().collectMessage("write size", total + "");
                LOG.info("Task finished, write size: {}", total);

            }
        }

        private void batchPut(final Record record, List points) {
                int size;
                if (ignoreWriteError) {
                    if (multiField) {
                        tsdb.multiFieldPut(points);
                    } else {
                        tsdb.put(points);
                    }
                } else {
                    if (multiField) {
                        retryForMput(record, points, ignoreWriteError, retrySize);
                    } else {
                        retryForPut(record, points, ignoreWriteError, retrySize);
                    }
                }
        }

        private void retryForPut(Record record, List<Point> points, boolean ignoreWriteError, int retrySize) {
            try {
                RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() {
                        int size;
                        DetailsResult summaryResult;
                        summaryResult = tsdb.putSync(points, DetailsResult.class);
                        if (summaryResult.getFailed() > 0) {

                            throw DataXException.asDataXException(TSDBWriterErrorCode.RUNTIME_EXCEPTION, "Write TSDB failed",
                                    new Exception(summaryResult.getErrors().toString()));
                        }

                        return 0;
                    }
                }, retrySize, 60000L, true);
            } catch (Exception e) {
                if (ignoreWriteError) {
                    LOG.error("Ignore write exceptions and continue writing.");
                } else {
                    getTaskPluginCollector().collectDirtyRecord(record, "Put data points failed!");
                    throw DataXException.asDataXException(TSDBWriterErrorCode.RETRY_WRITER_EXCEPTION, e);
                }
            }
        }

        private void retryForMput(Record record, List<MultiFieldPoint> points, boolean ignoreWriteError, int retrySize) {
            try {
                RetryUtil.executeWithRetry(new Callable<Integer>() {
                    @Override
                    public Integer call() {
                        int size;
                        MultiFieldDetailsResult summaryResult;
                        summaryResult = tsdb.multiFieldPutSync(points, MultiFieldDetailsResult.class);
                        if (summaryResult.getFailed() > 0) {
                            throw DataXException.asDataXException(TSDBWriterErrorCode.RUNTIME_EXCEPTION, "Write TSDB failed",
                                    new Exception(summaryResult.getErrors().toString()));
                        }

                        return 0;
                    }
                }, retrySize, 60000L, true);
            } catch (Exception e) {
                if (ignoreWriteError) {
                    LOG.error("Ignore write exceptions and continue writing.");
                } else {
                    getTaskPluginCollector().collectDirtyRecord(record, "Put data points failed!");
                    throw DataXException.asDataXException(TSDBWriterErrorCode.RETRY_WRITER_EXCEPTION, e);
                }
            }
        }

        private long doBatchInsert(final List<Record> writerBuffer) {
            int size;
            if (ignoreWriteError) {
                if (multiField) {
                    List<MultiFieldPoint> points = tsdbConverter.transRecord2MultiFieldPoint(writerBuffer, tableName);
                    size = points.size();
                    MultiFieldIgnoreErrorsResult ignoreErrorsResult = tsdb.multiFieldPutSync(points, MultiFieldIgnoreErrorsResult.class);
                    if (ignoreErrorsResult == null) {
                        LOG.error("Unexpected inner error for insert");
                    } else if (ignoreErrorsResult.getFailed() > 0) {
                        LOG.error("write TSDB failed num:" + ignoreErrorsResult.getFailed());
                    }
                } else {
                    List<Point> points = tsdbConverter.transRecord2Point(writerBuffer);
                    size = points.size();
                    IgnoreErrorsResult ignoreErrorsResult = tsdb.putSync(points, IgnoreErrorsResult.class);
                    if (ignoreErrorsResult == null) {
                        LOG.error("Unexpected inner error for insert");
                    } else if (ignoreErrorsResult.getFailed() > 0) {
                        LOG.error("write TSDB failed num:" + ignoreErrorsResult.getFailed());
                    }
                }
            } else {
                SummaryResult summaryResult;
                if (multiField) {
                    List<MultiFieldPoint> points = tsdbConverter.transRecord2MultiFieldPoint(writerBuffer, tableName);
                    size = points.size();
                    summaryResult = tsdb.multiFieldPutSync(points, SummaryResult.class);
                } else {
                    List<Point> points = tsdbConverter.transRecord2Point(writerBuffer);
                    size = points.size();
                    summaryResult = tsdb.putSync(points, SummaryResult.class);
                }
                if (summaryResult.getFailed() > 0) {
                    LOG.error("write TSDB failed num:" + summaryResult.getFailed());
                    throw DataXException.asDataXException(TSDBWriterErrorCode.RUNTIME_EXCEPTION, "Write TSDB failed", new Exception());
                }
            }
            return size;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
