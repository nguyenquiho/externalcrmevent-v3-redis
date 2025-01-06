/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 *
 * @author MitsuyoRai
 */
public class MysqlCDRConnection {

    final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");

    private static String dbHost = "127.0.0.1";
    private static int dbPort = 3306;
    private static String dbName = "cdr";
    private static String dbUsername = "userasterisk";
    private static String dbPassword = "stel7779";

    private final int maxNumberOfQuery = 2;

    private static ComboPooledDataSource getMySQLDataSource() {
        try {
            File file1 = new File(System.getProperty("user.dir"));
            URL[] urls = {file1.toURI().toURL()};
            ClassLoader loader = new URLClassLoader(urls);
            ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
            if (rb.containsKey("db.host")) {
                dbHost = rb.getString("db.host");
            } else {
                logger.error("Missing redis.host");
            }

            if (rb.containsKey("db.port")) {
                dbPort = Integer.parseInt(rb.getString("db.port"));
            } else {
                logger.error("Missing db.port");
            }

            if (rb.containsKey("db.user")) {
                dbUsername = rb.getString("db.user");
            } else {
                logger.error("Missing db.user");
            }

            if (rb.containsKey("db.pass")) {
                dbPassword = rb.getString("db.pass");
            } else {
                logger.error("Missing db.pass");
            }
        } catch (MalformedURLException ex) {
            logger.error(ex);
        }
        try {
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName + "?user=" + dbUsername + "&password=" + dbPassword + "&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
            dataSource.setJdbcUrl(url);
            dataSource.setMinPoolSize(10);
            dataSource.setMaxPoolSize(1000);
            dataSource.setAcquireIncrement(5);
            return dataSource;
        } catch (PropertyVetoException e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    private Connection conn;

    /**
     * constructor
     */
    private MysqlCDRConnection() {
        try {
            ComboPooledDataSource dataSource = MysqlCDRConnection.getMySQLDataSource();
            conn = dataSource.getConnection();
        } catch (SQLException ex) {
            logger.error("MysqlCDRConnection IOException: ", ex);
        }
    }

    private static volatile MysqlCDRConnection instance;

    public static MysqlCDRConnection getInstance() {
        MysqlCDRConnection _instance = MysqlCDRConnection.instance;
        if (_instance == null) {
            synchronized (MysqlCDRConnection.class) {
                _instance = MysqlCDRConnection.instance;
                if (_instance == null) {
                    MysqlCDRConnection.instance = _instance = new MysqlCDRConnection();
                }
            }
        }
        return _instance;
    }

    @Override
    protected void finalize() {
        try {
            if (conn != null) {
                conn.close();
            }

            MysqlCDRConnection.instance = null;

            super.finalize();
        } catch (Throwable ex) {
            logger.error("Throwable : ", ex);
        }
    }

    /**
     * bindParams
     *
     * @param pst
     * @param params
     * @throws SQLException
     */
    private void bindParams(PreparedStatement pst, Object... params) throws SQLException {
        int i = 0;
        for (Object item : params) {
            i++;
            if (item instanceof Integer) {
                pst.setInt(i, Integer.parseInt(item.toString()));
            }
            if (item instanceof String) {
                pst.setString(i, item.toString());
            }
            if (item == null) {
                pst.setString(i, null);
            }
        }
    }

    /**
     * getColumnByQuery
     *
     * @param sql
     * @param col
     * @return
     */
    public String getColumnByQuery(String sql, String col) {
        int retryCount = maxNumberOfQuery;
        String context = null;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql); ResultSet rs = pst.executeQuery()) {
                    if (rs.first()) {
                        context = rs.getString(col);
                    }
                }
                return context;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: ", ex);
            }
        } while (retryCount > 0);
        return null;
    }

    /**
     * getColumnByQuery
     *
     * @param sql
     * @param col
     * @param params
     * @return
     */
    public String getColumnByQuery(String sql, String col, Object... params) {
        int retryCount = maxNumberOfQuery;
        String context = null;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    bindParams(pst, params);
                    try (ResultSet rs = pst.executeQuery()) {
                        if (rs.first()) {
                            context = rs.getString(col);
                        }
                    }
                }
                return context;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: ", ex);
            }
        } while (retryCount > 0);
        return null;
    }

    /**
     * getListByQuery
     *
     * @param sql
     * @return
     */
    public ArrayList<HashMap> getListByQuery(String sql) {
        int retryCount = maxNumberOfQuery;
        ArrayList<HashMap> context = null;
        do {
            try {
                context = new ArrayList<>();
                try (PreparedStatement pst = conn.prepareStatement(sql); ResultSet rs = pst.executeQuery()) {
                    ResultSetMetaData rsmt = rs.getMetaData();
                    while (rs.next()) {
                        HashMap<String, String> row = new HashMap<>();
                        int i = 1;
                        while (i <= rsmt.getColumnCount()) {
                            row.put(rsmt.getColumnLabel(i), rs.getString(i));
                            i++;
                        }
                        context.add(row);
                    }
                }
                return context;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return null;
    }

    /**
     * getListByQuery
     *
     * @param sql
     * @param params
     * @return
     */
    public ArrayList<HashMap> getListByQuery(String sql, Object... params) {
        int retryCount = maxNumberOfQuery;
        ArrayList<HashMap> context = null;
        do {
            try {
                context = new ArrayList<>();
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    bindParams(pst, params);
                    try (ResultSet rs = pst.executeQuery()) {
                        ResultSetMetaData rsmt = rs.getMetaData();
                        while (rs.next()) {
                            HashMap<String, String> row = new HashMap<>();
                            int i = 1;
                            while (i <= rsmt.getColumnCount()) {
                                row.put(rsmt.getColumnLabel(i), rs.getString(i));
                                i++;
                            }
                            context.add(row);
                        }
                    }
                }
                return context;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return null;
    }

    /**
     * getOneByQuery
     *
     * @param sql
     * @return
     */
    public HashMap getOneByQuery(String sql) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql); ResultSet rs = pst.executeQuery()) {
                    ResultSetMetaData rsmt = rs.getMetaData();
                    if (rs.first()) {
                        HashMap<String, String> row = new HashMap<>();
                        int i = 1;
                        while (i <= rsmt.getColumnCount()) {
                            row.put(rsmt.getColumnLabel(i), rs.getString(i));
                            i++;
                        }
                        return row;
                    }
                }
                return null;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return null;
    }

    /**
     * getOneByQuery
     *
     * @param sql
     * @param params
     * @return
     */
    public HashMap<String, String> getOneByQuery(String sql, Object... params) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    bindParams(pst, params);
                    try (ResultSet rs = pst.executeQuery()) {
                        ResultSetMetaData rsmt = rs.getMetaData();
                        if (rs.first()) {
                            HashMap<String, String> row = new HashMap<>();
                            int i = 1;
                            while (i <= rsmt.getColumnCount()) {
                                row.put(rsmt.getColumnLabel(i), rs.getString(i));
                                i++;
                            }
                            return row;
                        }
                    }
                }
                return null;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return null;
    }

    /**
     * executeNonQuery
     *
     * @param sql
     */
    public void executeNonQuery(String sql) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    pst.execute();
                }
                return;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
    }

    /**
     * executeNonQuery
     *
     * @param sql
     * @param params
     */
    public void executeNonQuery(String sql, Object... params) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    bindParams(pst, params);
                    pst.execute();
                }
                return;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
    }

    /**
     * executeUpdateQuery
     *
     * @param sql
     * @return
     */
    public boolean executeUpdateQuery(String sql) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    int exec = pst.executeUpdate();
                    return (exec > 0);
                }
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if (retryCount > 0 && ("08S01".equals(sqlState) || "40001".equals(sqlState))) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Update Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return false;
    }

    /**
     * executeUpdateQuery
     *
     * @param sql
     * @param params
     * @return
     */
    public boolean executeUpdateQuery(String sql, Object... params) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    bindParams(pst, params);
                    int exec = pst.executeUpdate();
                    return (exec > 0);
                }
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Update Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return false;
    }

    /**
     * executeUpdateNonQuery
     *
     * @param sql
     */
    public void executeUpdateNonQuery(String sql) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    pst.executeUpdate();
                }
                return;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Update Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
    }

    /**
     * executeUpdateNonQuery
     *
     * @param sql
     * @param params
     */
    public void executeUpdateNonQuery(String sql, Object... params) {
        int retryCount = maxNumberOfQuery;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql)) {
                    bindParams(pst, params);
                    pst.executeUpdate();
                }
                return;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Update Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
    }

    /**
     * executeUpdateNonQueryTransaction
     *
     * @param sqls String[] sqls = {"SQL1 ?,?,?...", "SQL2 ?,?,?...",...};
     */
    public void executeUpdateNonQueryTransaction(String[] sqls) {
        int retryCount = maxNumberOfQuery;
        boolean transactionCompleted = false;
        do {
            try {
                conn.setAutoCommit(false);
                boolean flag = true;
                for (String sql : sqls) {
                    try (PreparedStatement pst = conn.prepareStatement(sql)) {
                        if (pst.executeUpdate() < 0) {
                            flag = false;
                        }
                    }
                }
                retryCount = 0;
                if (flag) {
                    conn.commit();
                } else {
                    conn.rollback();
                }
                transactionCompleted = true;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Update Error query: " + Arrays.toString(sqls), sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + Arrays.toString(sqls), ex);
            }
        } while (!transactionCompleted && retryCount > 0);
    }

    /**
     * executeUpdateNonQueryTransaction
     *
     * @param sqls String[] sqls = {"SQL1 ?,?,?...", "SQL2 ?,?,?...",...};
     * @param params Object[] param1 = {Integer, "String", Object,...}, Object[]
     * param2 = {String, "Integer", Object,...}, ...
     */
    public void executeUpdateNonQueryTransaction(String[] sqls, Object[]... params) {
        int retryCount = maxNumberOfQuery;
        boolean transactionCompleted = false;
        do {
            try {
                conn.setAutoCommit(false);
                boolean flag = true;
                int i = 0;
                for (String sql : sqls) {
                    try (PreparedStatement pst = conn.prepareStatement(sql)) {
                        bindParams(pst, params[i]);
                        if (pst.executeUpdate() < 0) {
                            flag = false;
                        }
                    }
                    i++;
                }
                retryCount = 0;
                if (flag) {
                    conn.commit();
                } else {
                    conn.rollback();
                }
                transactionCompleted = true;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("Execute Update Error query: " + Arrays.toString(sqls), sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + Arrays.toString(sqls), ex);
            }
        } while (!transactionCompleted && retryCount > 0);
    }
}
