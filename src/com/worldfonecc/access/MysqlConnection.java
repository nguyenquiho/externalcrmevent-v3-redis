/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.worldfonecc.access;

import com.mysql.cj.jdbc.MysqlDataSource;
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
public class MysqlConnection {

    private final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("CrmEvent");

    private String dbHost = "127.0.0.1";
    private int dbPort = 3306;
    private String dbName = "sipcloud";
    private String dbUsername = "userasterisk";
    private String dbPassword = "stel7779";

    private final int maxNumberOfQuery = 2;

    private Connection conn = null;

    private static volatile MysqlConnection instance;

    public static MysqlConnection getInstance() {
        MysqlConnection _instance = MysqlConnection.instance;
        if (_instance == null) {
            synchronized (MysqlConnection.class) {
                _instance = MysqlConnection.instance;
                if (_instance == null) {
                    MysqlConnection.instance = _instance = new MysqlConnection();
                }
            }
        }
        return _instance;
    }

    /**
     * constructor
     */
    private MysqlConnection() {
        try {
            File file1 = new File(System.getProperty("user.dir"));
            URL[] urls = {file1.toURI().toURL()};
            ClassLoader loader = new URLClassLoader(urls);
            ResourceBundle rb = ResourceBundle.getBundle("PBX", Locale.getDefault(), loader);
            if (rb.containsKey("db.host")) {
                this.dbHost = rb.getString("db.host");
            } else {
                logger.error("Missing redis.host");
            }

            if (rb.containsKey("db.port")) {
                this.dbPort = Integer.parseInt(rb.getString("db.port"));
            } else {
                logger.error("Missing db.port");
            }

            if (rb.containsKey("db.user")) {
                this.dbUsername = rb.getString("db.user");
            } else {
                logger.error("Missing db.user");
            }

            if (rb.containsKey("db.pass")) {
                this.dbPassword = rb.getString("db.pass");
            } else {
                logger.error("Missing db.pass");
            }

            if (rb.containsKey("db.name")) {
                this.dbName = rb.getString("db.name");
            } else {
                logger.error("Missing db.name");
            }
        } catch (MalformedURLException ex) {
            logger.error(ex);
        }

        MysqlDataSource ds = new MysqlDataSource();
        String url = "jdbc:mysql://" + this.dbHost + ":" + this.dbPort + "/" + this.dbName + "?user=" + this.dbUsername + "&password=" + this.dbPassword + "&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
        ds.setURL(url);
        try {
            conn = ds.getConnection();
        } catch (SQLException ex) {
            logger.error("SQLException: ", ex);
        }
    }

    @Override
    protected void finalize() {
        try {
            if (conn != null) {
                conn.close();
            }

            MysqlConnection.instance = null;

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
                try (PreparedStatement pst = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY); ResultSet rs = pst.executeQuery()) {
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
                    logger.error("SQL Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
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
                try (PreparedStatement pst = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
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
                    logger.error("SQL Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
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
    public ArrayList<String> getListColumnByQuery(String sql, String col, Object... params) {
        int retryCount = maxNumberOfQuery;
        ArrayList<String> outList = null;
        do {
            try {
                try (PreparedStatement pst = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    bindParams(pst, params);
                    try (ResultSet rs = pst.executeQuery()) {
                        outList = new ArrayList<>();
                        while (rs.next()) {
                            outList.add(rs.getString(col));
                        }
                    }
                }
                return outList;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("SQL Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return outList;
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
                try (PreparedStatement pst = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY); ResultSet rs = pst.executeQuery()) {
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
                    logger.error("SQL Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return context;
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
                    logger.error("SQL Error query: " + sql, sqlEx);
                }
            } catch (Exception ex) {
                retryCount = 0;
                logger.error("Error: " + sql, ex);
            }
        } while (retryCount > 0);
        return context;
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
                try (PreparedStatement pst = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY); ResultSet rs = pst.executeQuery()) {
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
                    logger.error("SQL Error query: " + sql, sqlEx);
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
                try (PreparedStatement pst = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
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
                    logger.error("SQL Error query: " + sql, sqlEx);
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
                retryCount = 0;
            } catch (SQLException sqlEx) {
                String sqlState = sqlEx.getSQLState();
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("SQL Error query: " + sql, sqlEx);
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
                    logger.error("SQL Error query: " + sql, sqlEx);
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
                if ("08S01".equals(sqlState) || "40001".equals(sqlState)) {
                    retryCount--;
                } else {
                    retryCount = 0;
                    logger.error("SQL Update Error query: " + sql, sqlEx);
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
                    logger.error("SQL Update Error query: " + sql, sqlEx);
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
                    logger.error("SQL Update Error query: " + sql, sqlEx);
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
                    logger.error("SQL Update Error query: " + sql, sqlEx);
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
