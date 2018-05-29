package com.redis.store.utils;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.*;

/**
 * Author LTY
 * Date 2018/05/24
 */
public class LogCreator {

    private static final Logger LOGGER = Logger.getLogger(LogCreator.class);

    private static final boolean BUFFERED_OUTPUT = false;

    private LogCreator() {
    }

    public static Logger createLogger(String name, String outputPath) {
        return createLogger(name, outputPath, new PatternLayout(), false, false);
    }

    public static Logger createLogger(String name, String outputPath, PatternLayout layout, boolean additivity) {
        return createLogger(name, outputPath, layout, additivity, false);
    }

    public static Logger createLogger(String name, String outputPath, PatternLayout layout, boolean additivity, boolean isDailyMode) {
        try {
            if (!StringUtils.isEmpty(outputPath)) {
                outputPath = outputPath.endsWith("/") ? outputPath : outputPath + "/";
            }

            String logPath = outputPath + name + ".log";
            FileAppender appender = isDailyMode ? new DailyRollingFileAppender(layout, logPath, "'.'yyyy-MM-dd") : new RollingFileAppender(layout, logPath, true);
            if (appender instanceof RollingFileAppender) {
                RollingFileAppender rollingAppender = (RollingFileAppender) appender;
                String maxFileSize = System.getProperty("logRollingSize", "20MB");

                rollingAppender.setMaxFileSize(maxFileSize);
                int maxBackups = NumberUtils.toInt(System.getProperty("logBackupNum", "5"));
                rollingAppender.setMaxBackupIndex(maxBackups);
            }

            if (BUFFERED_OUTPUT) {
                appender.setBufferedIO(true);
                appender.setBufferSize(8192);
            }

            Logger newLogger = Logger.getLogger(name);
            newLogger.removeAllAppenders();
            newLogger.addAppender(appender);
            newLogger.setAdditivity(additivity);
            return newLogger;
        } catch (Exception var11) {
            LOGGER.error("create Logger error! name: " + name, var11);
            throw new RuntimeException();
        }
    }
}
