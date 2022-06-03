/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.bithaus.medium.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC Connection pool
 * @author jmakuc
 */
public class JdbcConnectionPool {
    
    private static final Logger logger = LoggerFactory.getLogger(JdbcConnectionPool.class);
    
    private Config config;
    private final LinkedList<Connection> connections = new LinkedList<>();
 
    public JdbcConnectionPool(Config config) throws SQLException, ClassNotFoundException, IllegalArgumentException {

        logger.info("Instanciating driver " + config.getDriver());
        
        Class.forName(config.getDriver());        
        
        this.config = config;
        
        connections.add(getNewConnection());

    }

    public synchronized Connection getConnection() throws SQLException{

        if(connections.size() < 1)
            return getNewConnection();

        Connection connection = connections.remove();

        if(!connection.isClosed())
            return connection;

        return getNewConnection();
    }

    public synchronized void returnConnection(Connection connection) {

        try {

            if(connection.isClosed())
                return;

            connection.setAutoCommit(true);
            connections.add(connection);
        }
        catch(Exception e) {

            // descartada porque no es valida no se agrega al pool
            // y se crea una nueva
        }

    }

    private Connection getNewConnection() throws SQLException {

        return DriverManager.getConnection(config.getConnectionUrl(), config.getUsername(), config.getPassword());
    }
    
    public static class Config extends AbstractConfig {
        
        public static final String DRIVER_CONFIG = "JDBC.Driver";
        public static final String DRIVER_DOC = "JDBC Database driver";
        
        public static final String CONNECTION_URL_CONFIG = "JDBC.ConnectionURL";
        public static final String CONNECTION_URL_DOC = "Database connection URL";
        
        public static final String USERNAME_CONFIG = "JDBC.Username";
        public static final String USERNAME_DOC = "Database Username";
        
        public static final String PASSWORD_CONFIG = "JDBC.Password";
        public static final String PASSWORD_DOC = "Database Password";
        
        public Config(Map originals) {
            super(getConfig(), originals);
        }
        
        public static ConfigDef getConfig() {
            
            return new ConfigDef()
                    .define(DRIVER_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, DRIVER_DOC)
                    .define(CONNECTION_URL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, CONNECTION_URL_DOC)
                    .define(USERNAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, USERNAME_DOC)
                    .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, PASSWORD_DOC)
                    ;
        }
        
        public String getDriver() {
            return this.getString(DRIVER_CONFIG);
        }
        
        public String getConnectionUrl() {
            return this.getString(CONNECTION_URL_CONFIG);
        }
        
        public String getUsername() {
            return this.getString(USERNAME_CONFIG);            
        }
        
        public String getPassword() {
            return this.getPassword(PASSWORD_CONFIG).value();
        }
        
    }
    
}
