package com.yaoqi.hbase.component.config;

import com.yaoqi.hbase.component.constant.KerberosConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author YaoQi
 * Date 2018/8/13 11:32
 * Modified
 * Description HBase配置类
 */
@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "HBase.conf")
@ComponentScan(value = "com.semptian.base.hbasecomponent")
public class HBaseConfig {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConfig.class);

    private String quorum;
    private String znodeParent;
    private Map<String, String> config;
    private User user = null;
    private volatile Connection connection;
    private static ConcurrentLinkedQueue<Connection> queue = new ConcurrentLinkedQueue<>();

    /**
     * 初始化配置
     *
     * @return
     * @throws IOException
     */
    private Configuration configuration() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        logger.info("zookeeper node : {}", quorum);
        logger.info("znodeParent is : {}", znodeParent);
        configuration.set("hbase.zookeeper.quorum", quorum);
        configuration.set("zookeeper.znode.parent", znodeParent);
        if (config != null && config.containsKey(KerberosConstant.AUTH_METHOD) && KerberosConstant.KERBEROS.equals(config.get(KerberosConstant.AUTH_METHOD))) {
            String keytabPath = config.get("user-keytab");
            String masterPrincipal = config.get("masterPrincipal");
            String regionserverPrincipal = config.get("regionserverPrincipal");
            String hbaseSitePath = config.get("hbaseSitePath");
            String coreSitePath = config.get("coreSitePath");
            configuration.set("hadoop.security.authentication", "kerberos");
            configuration.set("hbase.security.authentication", "kerberos");
            if (StringUtils.isNotBlank(hbaseSitePath)) {
                configuration.addResource(new Path(hbaseSitePath));
            }
            if (StringUtils.isNotBlank(coreSitePath)) {
                configuration.addResource(new Path(coreSitePath));
            }
            if (StringUtils.isNotBlank(masterPrincipal) && StringUtils.isNotBlank(regionserverPrincipal)) {
                configuration.set("hbase.master.kerberos.principal", masterPrincipal);
                configuration.set("hbase.regionserver.kerberos.principal", regionserverPrincipal);
            }
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(masterPrincipal, keytabPath);
            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
            user = User.create(loginUser);
        }
        // 将config中的配置加入到configuration中
        if (config != null && !config.isEmpty()) {
            config.forEach(configuration::set);
        }
        return configuration;
    }

    /**
     * 重新初始化连接，解决认证过期问题，默认过期时间为24h，因此reInitial() 的执行周期应该小于24h
     * 认证环境下才会开启
     */
    @PostConstruct
    private void reInitial() {
        int refreshAuth = 12;
        boolean openReInitial = false;
        if (config != null && !config.isEmpty()) {
            String refreshAuthConfig = config.get(KerberosConstant.REFRESH_AUTH);
            if (StringUtils.isNotEmpty(refreshAuthConfig)) {
                // 如果有配置，则按照配置的时间进行刷新
                refreshAuth = Integer.valueOf(refreshAuthConfig);
            }
            String authMethod = config.get(KerberosConstant.AUTH_METHOD);
            if (KerberosConstant.KERBEROS.equals(authMethod)) {
                openReInitial = true;
            }
        }
        if (openReInitial) {
            // 认证环境下才会开启
            logger.info("create new thread refresh connection.");
            ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                    new BasicThreadFactory.Builder().namingPattern("renew-connection-pool-%d").daemon(true).build());
            final int finalRefreshAuth = refreshAuth;
            executorService.submit((Runnable) () -> {
                while (true) {
                    try {
                        TimeUnit.HOURS.sleep(finalRefreshAuth);
                    } catch (InterruptedException e) {
                        logger.error("reLogin kerberos error ,error info : {}", e.getMessage());
                    }
                    initial();
                    closeOldConnection();
                    logger.info("renew connection:{}", connection);
                }
            });
        }
    }

    /**
     * 获取连接
     *
     * @return
     */
    public Connection getConnection() {
        if (connection == null) {
            connection = initial();
        }
        return connection;
    }

    /**
     * 初始化连接，如果存在老的连接则加入队列
     *
     * @return
     */
    @PostConstruct
    private Connection initial() {
        Connection newConnection;
        try {
            newConnection = ConnectionFactory.createConnection(configuration(), user);
            Connection oldConnection = connection;
            if (newConnection != null) {
                connection = newConnection;
                if (oldConnection != null) {
                    queue.add(oldConnection);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭老的连接
     */
    private void closeOldConnection() {
        if (queue != null && !queue.isEmpty() && queue.size() > 1) {
            // 在创建connection的第二个周期开始清空队列
            Connection oldConnection = queue.poll();
            if (oldConnection != null && !oldConnection.isClosed()) {
                try {
                    oldConnection.close();
                } catch (IOException e) {
                    logger.error("oldConnection close failed.");
                    e.printStackTrace();
                }
            }
        }
    }

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public String getZnodeParent() {
        return znodeParent;
    }

    public void setZnodeParent(String znodeParent) {
        this.znodeParent = znodeParent;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}
