package com.yaoqi.hbase.component.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.Map;

/**
 * @Author YaoQi
 * @Date 2018/8/13 11:32
 * @Modified
 * @Description HBase配置类
 */
@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "HBase.conf")
public class HBaseConfig {

    private static final Logger logger = LoggerFactory.getLogger(HBaseConfig.class);

    private String quorum;
    private String znodeParent;
    private Map<String, String> config;
    private User user = null;

    @Bean
    public Configuration configuration() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        logger.info("zookeeper node : {}", quorum);
        logger.info("znodeParent is : {}", znodeParent);
        configuration.set("hbase.zookeeper.quorum", quorum);
        configuration.set("zookeeper.znode.parent", znodeParent);

        if (config != null && config.containsKey("authMethod") && "kerberos".equals(config.get("authMethod"))) {
            String keytabPath = config.get("user-keytab");
            String masterPrincipal = config.get("masterPrincipal");
            String regionServerPrincipal = config.get("regionserverPrincipal");
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
            if (StringUtils.isNotBlank(masterPrincipal) && StringUtils.isNotBlank(regionServerPrincipal)) {
                configuration.set("hbase.master.kerberos.principal", masterPrincipal);
                configuration.set("hbase.regionserver.kerberos.principal", regionServerPrincipal);
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

    @Bean
    public Connection getConnection() {
        Connection connection = null;
        try {
            if (user == null) {
                connection = ConnectionFactory.createConnection(configuration());
            } else {
                connection = ConnectionFactory.createConnection(configuration(), user);
            }
        } catch (IOException e) {
            logger.info("get baseAdmin exception {}", e.getMessage());
            e.printStackTrace();
        }
        return connection;
    }

    @Bean
    public HBaseAdmin getHBaseAdmin() {
        try {
            Connection connection = getConnection();
            return (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            logger.info("get baseAdmin exception {}", e.getMessage());
            e.printStackTrace();
        }
        return null;
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
