/**
 * IK 中文分词  版本 5.0
 * IK Analyzer release 5.0
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * 源代码由林良益(linliangyi2005@gmail.com)提供
 * 版权声明 2012，乌龙茶工作室
 * provided by Linliangyi and copyright 2012 by Oolong studio
 */

package org.wltea.analyzer.dic;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.plugin.analysis.ik.AnalysisIkPlugin;
import org.wltea.analyzer.cfg.Configuration;
import org.wltea.analyzer.help.ESPluginLoggerFactory;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 词典管理类,单子模式
 */
public class Dictionary {

    /*
     * 词典单子实例
     */
    private static Dictionary singleton;

    private DictSegment _MainDict;

    private DictSegment _QuantifierDict;

    private DictSegment _StopWords;

    /**
     * 配置对象
     */
    private Configuration configuration;

    private static final Logger logger = ESPluginLoggerFactory.getLogger(Dictionary.class.getName());

    private static ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    private static final String PATH_DIC_MAIN = "main.dic";

    private static final String PATH_DIC_SURNAME = "surname.dic";

    private static final String PATH_DIC_QUANTIFIER = "quantifier.dic";

    private static final String PATH_DIC_SUFFIX = "suffix.dic";

    private static final String PATH_DIC_PREP = "preposition.dic";

    private static final String PATH_DIC_STOP = "stopword.dic";

    private final static String FILE_NAME = "IKAnalyzer.cfg.xml";

    private final static String EXT_DICT = "ext_dict";

    private final static String REMOTE_EXT_DICT = "remote_ext_dict";

    private final static String EXT_STOP = "ext_stopwords";

    private final static String REMOTE_EXT_STOP = "remote_ext_stopwords";

    private Path conf_dir;

    private Properties props;

    private Dictionary(Configuration cfg) {
        this.configuration = cfg;
        this.props         = new Properties();
        this.conf_dir      = cfg.getEnvironment().configFile().resolve(AnalysisIkPlugin.PLUGIN_NAME);

        Path configFile = conf_dir.resolve(FILE_NAME);

        InputStream input = null;
        try {
            logger.info("try load config from {}", configFile);
            input = new FileInputStream(configFile.toFile());
        } catch (FileNotFoundException e) {
            conf_dir   = cfg.getConfigInPluginDir();
            configFile = conf_dir.resolve(FILE_NAME);
            try {
                logger.info("try load config from {}", configFile);
                input = new FileInputStream(configFile.toFile());
            } catch (FileNotFoundException ex) {
                // We should report origin exception
                logger.error("ik-analyzer", e);
            }
        }
        if (input != null) {
            try {
                props.loadFromXML(input);
            } catch (IOException e) {
                logger.error("ik-analyzer", e);
            }
        }
    }

    private String getProperty(String key) {
        if (props != null) {
            return props.getProperty(key);
        }
        return null;
    }

    /**
     * 词典初始化 由于IK Analyzer的词典采用Dictionary类的静态方法进行词典初始化
     * 只有当Dictionary类被实际调用时，才会开始载入词典， 这将延长首次分词操作的时间 该方法提供了一个在应用加载阶段就初始化字典的手段
     * @return Dictionary
     */
    public static synchronized void initial(Configuration cfg) {
        if (singleton == null) {
            synchronized (Dictionary.class) {
                if (singleton == null) {

                    singleton = new Dictionary(cfg);
                    singleton.loadMainDict();
                    singleton.loadSurnameDict();
                    singleton.loadQuantifierDict();
                    singleton.loadSuffixDict();
                    singleton.loadPrepDict();
                    singleton.loadStopWordDict();

                    if (cfg.isEnableRemoteDict()) {
                        // 建立监控线程
                        for (String location : singleton.getRemoteExtDictionarys()) {
                            // 10 秒是初始延迟可以修改的 60是间隔时间 单位秒
                            pool.scheduleAtFixedRate(new Monitor(location), 10, 60, TimeUnit.SECONDS);
                        }
                        for (String location : singleton.getRemoteExtStopWordDictionarys()) {
                            pool.scheduleAtFixedRate(new Monitor(location), 10, 60, TimeUnit.SECONDS);
                        }
                    }

                    pool.scheduleAtFixedRate(new DbMonitor(), 10, 60, TimeUnit.SECONDS);
                }
            }
        }
    }

    private void walkFileTree(List<String> files, Path path) {
        if (Files.isRegularFile(path)) {
            files.add(path.toString());
        } else if (Files.isDirectory(path)) try {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    files.add(file.toString());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e) {
                    logger.error("[Ext Loading] listing files", e);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            logger.error("[Ext Loading] listing files", e);
        }
        else {
            logger.warn("[Ext Loading] file not found: " + path);
        }
    }

    private void loadDictFile(DictSegment dict, Path file, boolean critical, String name) {
        try (InputStream is = new FileInputStream(file.toFile())) {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(is, "UTF-8"), 512);
            String word = br.readLine();
            if (word != null) {
                if (word.startsWith("\uFEFF"))
                    word = word.substring(1);
                for (; word != null; word = br.readLine()) {
                    word = word.trim();
                    if (word.isEmpty()) continue;
                    dict.fillSegment(word.toCharArray());
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("ik-analyzer: " + name + " not found", e);
            if (critical) throw new RuntimeException("ik-analyzer: " + name + " not found!!!", e);
        } catch (IOException e) {
            logger.error("ik-analyzer: " + name + " loading failed", e);
        }
    }

    private List<String> getExtDictionarys() {
        List<String> extDictFiles = new ArrayList<String>(2);
        String       extDictCfg   = getProperty(EXT_DICT);
        if (extDictCfg != null) {

            String[] filePaths = extDictCfg.split(";");
            for (String filePath : filePaths) {
                if (filePath != null && !"".equals(filePath.trim())) {
                    Path file = PathUtils.get(getDictRoot(), filePath.trim());
                    walkFileTree(extDictFiles, file);

                }
            }
        }
        return extDictFiles;
    }

    private List<String> getRemoteExtDictionarys() {
        List<String> remoteExtDictFiles = new ArrayList<String>(2);
        String       remoteExtDictCfg   = getProperty(REMOTE_EXT_DICT);
        if (remoteExtDictCfg != null) {

            String[] filePaths = remoteExtDictCfg.split(";");
            for (String filePath : filePaths) {
                if (filePath != null && !"".equals(filePath.trim())) {
                    remoteExtDictFiles.add(filePath);

                }
            }
        }
        return remoteExtDictFiles;
    }

    private List<String> getExtStopWordDictionarys() {
        List<String> extStopWordDictFiles = new ArrayList<String>(2);
        String       extStopWordDictCfg   = getProperty(EXT_STOP);
        if (extStopWordDictCfg != null) {

            String[] filePaths = extStopWordDictCfg.split(";");
            for (String filePath : filePaths) {
                if (filePath != null && !"".equals(filePath.trim())) {
                    Path file = PathUtils.get(getDictRoot(), filePath.trim());
                    walkFileTree(extStopWordDictFiles, file);

                }
            }
        }
        return extStopWordDictFiles;
    }

    private List<String> getRemoteExtStopWordDictionarys() {
        List<String> remoteExtStopWordDictFiles = new ArrayList<String>(2);
        String       remoteExtStopWordDictCfg   = getProperty(REMOTE_EXT_STOP);
        if (remoteExtStopWordDictCfg != null) {

            String[] filePaths = remoteExtStopWordDictCfg.split(";");
            for (String filePath : filePaths) {
                if (filePath != null && !"".equals(filePath.trim())) {
                    remoteExtStopWordDictFiles.add(filePath);

                }
            }
        }
        return remoteExtStopWordDictFiles;
    }

    private String getDictRoot() {
        return conf_dir.toAbsolutePath().toString();
    }

    /**
     * 获取词典单子实例
     * @return Dictionary 单例对象
     */
    public static Dictionary getSingleton() {
        if (singleton == null) {
            throw new IllegalStateException("ik dict has not been initialized yet, please call initial method first.");
        }
        return singleton;
    }

    /**
     * 批量加载新词条
     * @param words Collection<String>词条列表
     */
    public void addWords(Collection<String> words) {
        if (words != null) {
            for (String word : words) {
                if (word != null) {
                    // 批量加载词条到主内存词典中
                    singleton._MainDict.fillSegment(word.trim().toCharArray());
                }
            }
        }
    }

    /**
     * 批量移除（屏蔽）词条
     */
    public void disableWords(Collection<String> words) {
        if (words != null) {
            for (String word : words) {
                if (word != null) {
                    // 批量屏蔽词条
                    singleton._MainDict.disableSegment(word.trim().toCharArray());
                }
            }
        }
    }

    /**
     * 检索匹配主词典
     * @return Hit 匹配结果描述
     */
    public Hit matchInMainDict(char[] charArray) {
        return singleton._MainDict.match(charArray);
    }

    /**
     * 检索匹配主词典
     * @return Hit 匹配结果描述
     */
    public Hit matchInMainDict(char[] charArray, int begin, int length) {
        return singleton._MainDict.match(charArray, begin, length);
    }

    /**
     * 检索匹配量词词典
     * @return Hit 匹配结果描述
     */
    public Hit matchInQuantifierDict(char[] charArray, int begin, int length) {
        return singleton._QuantifierDict.match(charArray, begin, length);
    }

    /**
     * 从已匹配的Hit中直接取出DictSegment，继续向下匹配
     * @return Hit
     */
    public Hit matchWithHit(char[] charArray, int currentIndex, Hit matchedHit) {
        DictSegment ds = matchedHit.getMatchedDictSegment();
        return ds.match(charArray, currentIndex, 1, matchedHit);
    }

    /**
     * 判断是否是停止词
     * @return boolean
     */
    public boolean isStopWord(char[] charArray, int begin, int length) {
        return singleton._StopWords.match(charArray, begin, length).isMatch();
    }

    /**
     * 加载主词典及扩展词典
     */
    private void loadMainDict() {
        // 建立一个主词典实例
        _MainDict = new DictSegment((char) 0);

        // 读取主词典文件
        Path file = PathUtils.get(getDictRoot(), Dictionary.PATH_DIC_MAIN);
        loadDictFile(_MainDict, file, false, "Main Dict");
        // 加载扩展词典
        this.loadExtDict();
        // 加载远程自定义词库
        this.loadRemoteExtDict();
        // 加载远程自定义数据库拓展词典到主词库表
        this.loadExtendDictFromMysql();
    }

    /**
     * 加载用户配置的扩展词典到主词库表
     */
    private void loadExtDict() {
        // 加载扩展词典配置
        List<String> extDictFiles = getExtDictionarys();
        if (extDictFiles != null) {
            for (String extDictName : extDictFiles) {
                // 读取扩展词典文件
                logger.info("[Dict Loading] " + extDictName);
                Path file = PathUtils.get(extDictName);
                loadDictFile(_MainDict, file, false, "Extra Dict");
            }
        }
    }

    /**
     * 加载远程扩展词典到主词库表
     */
    private void loadRemoteExtDict() {
        List<String> remoteExtDictFiles = getRemoteExtDictionarys();
        for (String location : remoteExtDictFiles) {
            logger.info("[Dict Loading] " + location);
            List<String> lists = getRemoteWords(location);
            // 如果找不到扩展的字典，则忽略
            if (lists == null) {
                logger.error("[Dict Loading] " + location + " load failed");
                continue;
            }
            for (String theWord : lists) {
                if (theWord != null && !"".equals(theWord.trim())) {
                    // 加载扩展词典数据到主内存词典中
                    logger.info(theWord);
                    _MainDict.fillSegment(theWord.trim().toLowerCase().toCharArray());
                }
            }
        }

    }

    private static List<String> getRemoteWords(String location) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<List<String>>) () -> {
            return getRemoteWordsUnprivileged(location);
        });
    }

    /**
     * 从远程服务器上下载自定义词条
     */
    private static List<String> getRemoteWordsUnprivileged(String location) {

        List<String> buffer = new ArrayList<String>();
        RequestConfig rc = RequestConfig.custom().setConnectionRequestTimeout(10 * 1000).setConnectTimeout(10 * 1000)
                                        .setSocketTimeout(60 * 1000).build();
        CloseableHttpClient   httpclient = HttpClients.createDefault();
        CloseableHttpResponse response;
        BufferedReader        in;
        HttpGet               get        = new HttpGet(location);
        get.setConfig(rc);
        try {
            response = httpclient.execute(get);
            if (response.getStatusLine().getStatusCode() == 200) {

                String charset = "UTF-8";
                // 获取编码，默认为utf-8
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    Header contentType = entity.getContentType();
                    if (contentType != null && contentType.getValue() != null) {
                        String typeValue = contentType.getValue();
                        if (typeValue != null && typeValue.contains("charset=")) {
                            charset = typeValue.substring(typeValue.lastIndexOf("=") + 1);
                        }
                    }

                    if (entity.getContentLength() > 0 || entity.isChunked()) {
                        in = new BufferedReader(new InputStreamReader(entity.getContent(), charset));
                        String line;
                        while ((line = in.readLine()) != null) {
                            buffer.add(line);
                        }
                        in.close();
                        response.close();
                        return buffer;
                    }
                }
            }
            response.close();
        } catch (IllegalStateException | IOException e) {
            logger.error("getRemoteWords {} error", e, location);
        }
        return buffer;
    }

    /**
     * 加载用户扩展的停止词词典
     */
    private void loadStopWordDict() {
        // 建立主词典实例
        _StopWords = new DictSegment((char) 0);

        // 读取主词典文件
        Path file = PathUtils.get(getDictRoot(), Dictionary.PATH_DIC_STOP);
        loadDictFile(_StopWords, file, false, "Main Stopwords");

        // 加载扩展停止词典
        List<String> extStopWordDictFiles = getExtStopWordDictionarys();
        if (extStopWordDictFiles != null) {
            for (String extStopWordDictName : extStopWordDictFiles) {
                logger.info("[Dict Loading] " + extStopWordDictName);

                // 读取扩展词典文件
                file = PathUtils.get(extStopWordDictName);
                loadDictFile(_StopWords, file, false, "Extra Stopwords");
            }
        }

        // 加载远程停用词典
        List<String> remoteExtStopWordDictFiles = getRemoteExtStopWordDictionarys();
        for (String location : remoteExtStopWordDictFiles) {
            logger.info("[Dict Loading] " + location);
            List<String> lists = getRemoteWords(location);
            // 如果找不到扩展的字典，则忽略
            if (lists == null) {
                logger.error("[Dict Loading] " + location + " load failed");
                continue;
            }
            for (String theWord : lists) {
                if (theWord != null && !"".equals(theWord.trim())) {
                    // 加载远程词典数据到主内存中
                    logger.info(theWord);
                    _StopWords.fillSegment(theWord.trim().toLowerCase().toCharArray());
                }
            }
        }

        // 从 MySQL 加载停用词
        this.loadStopDictFromMysql();
    }

    /**
     * 加载量词词典
     */
    private void loadQuantifierDict() {
        // 建立一个量词典实例
        _QuantifierDict = new DictSegment((char) 0);
        // 读取量词词典文件
        Path file = PathUtils.get(getDictRoot(), Dictionary.PATH_DIC_QUANTIFIER);
        loadDictFile(_QuantifierDict, file, false, "Quantifier");
    }

    private void loadSurnameDict() {
        DictSegment _SurnameDict = new DictSegment((char) 0);
        Path        file         = PathUtils.get(getDictRoot(), Dictionary.PATH_DIC_SURNAME);
        loadDictFile(_SurnameDict, file, true, "Surname");
    }

    private void loadSuffixDict() {
        DictSegment _SuffixDict = new DictSegment((char) 0);
        Path        file        = PathUtils.get(getDictRoot(), Dictionary.PATH_DIC_SUFFIX);
        loadDictFile(_SuffixDict, file, true, "Suffix");
    }

    private void loadPrepDict() {
        DictSegment _PrepDict = new DictSegment((char) 0);
        Path        file      = PathUtils.get(getDictRoot(), Dictionary.PATH_DIC_PREP);
        loadDictFile(_PrepDict, file, true, "Preposition");
    }

    void reLoadMainDict() {
        logger.info("start to reload ik dict.");
        // 新开一个实例加载词典，减少加载过程对当前词典使用的影响
        Dictionary tmpDict = new Dictionary(configuration);
        tmpDict.configuration = getSingleton().configuration;
        tmpDict.loadMainDict();
        tmpDict.loadStopWordDict();
        _MainDict  = tmpDict._MainDict;
        _StopWords = tmpDict._StopWords;
        logger.info("reload ik dict finished.");
    }

    /**
     * 加载属性配置
     * Author: Sweeper <wili.lixiang@gmail.com>
     * DateTime: 2024/2/23 14:04
     */
    public Dictionary loadingJdbcReloadProperties() {
        try {
            logger.info("----- loading jdbc-reload.properties START -----");

            Path file = PathUtils.get(getDictRoot(), "jdbc-reload.properties");
            props.load(Files.newInputStream(file.toFile().toPath()));
            // props.load(new FileInputStream(file.toFile()));

            for (Object key : props.keySet()) {
                logger.info(key + "=" + props.getProperty(String.valueOf(key)));
            }
            logger.info("----- loading jdbc-reload.properties END -----");
        } catch (Throwable e) {
            logger.error("----- loading jdbc-reload.properties ERROR -----", e);
        }

        return this;
    }

    /**
     * 连接数据库
     * Author: Sweeper <wili.lixiang@gmail.com>
     * DateTime: 2024/2/23 14:05
     * @return Connection
     */
    public Connection connection(Boolean loadingJdbcReloadProperties) {
        String url      = props.getProperty("jdbc.url");
        String username = props.getProperty("jdbc.username");
        String password = props.getProperty("jdbc.password");
        if (loadingJdbcReloadProperties || url == null || url.trim().isEmpty() || username == null || username.trim().isEmpty() || password == null || password.trim().isEmpty()) {
            this.loadingJdbcReloadProperties();

            url      = props.getProperty("jdbc.url");
            username = props.getProperty("jdbc.username");
            password = props.getProperty("jdbc.password");
        }

        return this.getConnection(url, username, password);
    }

    /**
     * 获取数据库连接
     * Author: Sweeper <wili.lixiang@gmail.com>
     * DateTime: 2024/2/23 14:07
     * @return Connection
     */
    public Connection getConnection(String url, String username, String password) {
        Connection connection = null;
        try {
            logger.info("----- Database connection initialize -----");
            connection = DriverManager.getConnection(url, username, password);
            logger.info("----- Database connection successful -----");
        } catch (SQLException e) {
            logger.error("----- Database connection error -----", e);
        }

        return connection; // 按方法要求返回一个Connection对象
    }

    /**
     * 查询总记录数
     * Author: Sweeper <wili.lixiang@gmail.com>
     * DateTime: 2024/2/23 14:31
     * @return count总记录数
     */
    public int count(String sql) {
        int               count = 0;//总记录数
        Connection        conn  = null; // 调用连接数据库的方法
        PreparedStatement pstmt = null;// PreparedStatement
        ResultSet         rs    = null;// 结果集
        try {
            conn  = this.connection(true);//获取数据库连接
            pstmt = conn.prepareStatement(sql);//获取PreparedStatement
            rs    = pstmt.executeQuery();//执行查询操作
            if (rs.next()) {//光标向后移动，并判断是否有效
                count = rs.getInt(1);//对总记录数赋值
            }
            rs.close();
            pstmt.close();
            conn.close();
        } catch (Throwable e) {
            logger.error("count error", e);
        }

        return count;//返回总记录数
    }

    /**
     * 加载自定义数据库拓展词典到主词库表
     * Author: Sweeper <wili.lixiang@gmail.com>
     * DateTime: 2024/2/23 14:08
     */
    public void loadExtendDictFromMysql() {
        logger.info("============================ loadExtendDictFromMysql START ============================");

        Connection connection = null;
        Statement  statement  = null;
        ResultSet  resultSet  = null;
        String     extendSql;
        String     extendCountSql;
        String     extendLimitSql;
        int        extendCount;
        try {
            connection     = this.connection(true);
            extendSql      = props.getProperty("jdbc.reload.extend.sql");
            extendCountSql = props.getProperty("jdbc.reload.extend.count.sql");
            extendLimitSql = props.getProperty("jdbc.reload.extend.limit.sql");
            statement      = connection.createStatement();
            // resultSet      = statement.executeQuery(extendSql);
            extendCount = this.count(extendCountSql);

            logger.info("hot dict executeQuery {}, extendCount({}[{}])", extendSql, extendCountSql, extendCount);

            // while (resultSet.next()) {
            //     // 加载扩展词典数据到主内存词典中
            //     String theWord = resultSet.getString("word");
            //     logger.info(theWord);
            //     _MainDict.fillSegment(theWord.trim().toLowerCase().toCharArray());
            // }

            int               page  = 1;
            int               limit;
            int               size  = Integer.parseInt(props.get("jdbc.page.size").toString());
            PreparedStatement pstmt = connection.prepareStatement(extendLimitSql);// 获取PreparedStatement
            while (true) {
                limit = (page - 1) * size;
                pstmt.setInt(1, limit);// 对SQL语句第一个参数赋值
                pstmt.setInt(2, size);// 对SQL语句第二个参数赋值
                resultSet = pstmt.executeQuery();// 执行查询操作

                logger.info("===== page sql: " + pstmt + " =====");

                if (!resultSet.isBeforeFirst()) {
                    logger.info("===== 暂无拓展词典数据 =====");
                    break;
                }
                page++;
                int rowCount = 0;
                while (resultSet.next()) {
                    // 加载扩展词典数据到主内存词典中
                    String theWord = resultSet.getString("word");
                    logger.info(theWord);
                    _MainDict.fillSegment(theWord.trim().toLowerCase().toCharArray());
                    rowCount++;
                }
                if (rowCount < size) {
                    logger.info("===== 暂无拓展词典数据 =====");
                    break;
                }
            }

            // 加载时间
            Thread.sleep(Integer.parseInt(String.valueOf(props.get("jdbc.reload.interval"))));
        } catch (Exception e) {
            logger.error("[Extend Dict Loading] " + e);
        } finally {
            if (resultSet != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("[Extend Dict Loading] " + e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("[Extend Dict Loading] " + e);
                }
            }

            logger.info("============================ loadExtendDictFromMysql FINISH ============================");
        }
    }

    /**
     * 加载自定义数据库拓展停止词词典到主词库表
     * Author: Sweeper <wili.lixiang@gmail.com>
     * DateTime: 2024/2/23 14:09
     */
    public void loadStopDictFromMysql() {
        logger.info("============================ loadStopDictFromMysql START ============================");
        // 建立主词典实例
        _StopWords = new DictSegment((char) 0);
        Connection connection = null;
        Statement  statement  = null;
        ResultSet  resultSet  = null;
        String     stopSql;
        String     stopCountSql;
        String     stopLimitSql;
        int        stopCount;
        try {
            connection   = this.connection(true);
            stopSql      = props.getProperty("jdbc.reload.stop.sql");
            stopCountSql = props.getProperty("jdbc.reload.stop.count.sql");
            stopLimitSql = props.getProperty("jdbc.reload.stop.limit.sql");
            statement    = connection.createStatement();
            // resultSet    = statement.executeQuery(stopSql);
            stopCount = this.count(stopCountSql);

            logger.info("stop dict executeQuery {}, stopCount({}[{}])", stopSql, stopCountSql, stopCount);

            // while (resultSet.next()) {
            //     // 加载扩展词典数据到主内存词典中
            //     String theWord = resultSet.getString("word");
            //     logger.info(theWord);
            //     _StopWords.fillSegment(theWord.trim().toLowerCase().toCharArray());
            // }

            int               page  = 1;
            int               limit;
            int               size  = Integer.parseInt(props.get("jdbc.page.size").toString());
            PreparedStatement pstmt = connection.prepareStatement(stopLimitSql);// 获取PreparedStatement
            while (true) {
                limit = (page - 1) * size;
                pstmt.setInt(1, limit);// 对SQL语句第一个参数赋值
                pstmt.setInt(2, size);// 对SQL语句第二个参数赋值
                resultSet = pstmt.executeQuery();// 执行查询操作

                logger.info("===== page sql: " + pstmt + " =====");

                if (!resultSet.isBeforeFirst()) {
                    logger.info("===== 暂无拓展停止词词典数据 =====");
                    break;
                }
                page++;
                int rowCount = 0;
                while (resultSet.next()) {
                    // 加载扩展词典数据到主内存词典中
                    String theWord = resultSet.getString("word");
                    logger.info(theWord);
                    _StopWords.fillSegment(theWord.trim().toLowerCase().toCharArray());
                    rowCount++;
                }
                if (rowCount < size) {
                    logger.info("===== 暂无拓展停止词词典数据 =====");
                    break;
                }
            }

            // 加载时间
            Thread.sleep(Integer.parseInt(String.valueOf(props.get("jdbc.reload.interval"))));
        } catch (Exception e) {
            logger.error("[Stop Dict Loading] " + e);
        } finally {
            if (resultSet != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    logger.error("[Stop Dict Loading] " + e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("[Stop Dict Loading] " + e);
                }
            }
            logger.info("============================ loadStopDictFromMysql FINISH ============================");
        }
    }

    /*
      MySQL 驱动加载
      Author: Sweeper <wili.lixiang@gmail.com>
      DateTime: 2024/2/23 14:09
     */
    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Database driver loaded successfully");
        } catch (ClassNotFoundException e) {
            logger.error("error", e);
        }
    }

}