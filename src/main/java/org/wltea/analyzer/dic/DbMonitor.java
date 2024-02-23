package org.wltea.analyzer.dic;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * 数据库监控程序
 */
public class DbMonitor implements Runnable {

    private static final Logger logger = ESPluginLoggerFactory.getLogger(DbMonitor.class.getName());

    public DbMonitor() {}

    public void run() {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                // 远程词库有更新,需要重新加载词典，并修改last_modified,eTags
                logger.info("------------ Dictionary reLoadMainDict START ------------");
                Dictionary.getSingleton().reLoadMainDict();
                logger.info("------------ Dictionary reLoadMainDict END ------------");
            } catch (Exception e) {
                logger.error("------------ Dictionary reLoadMainDict ERROR! ------------", e);
            }
            return null;
        });
    }

}
