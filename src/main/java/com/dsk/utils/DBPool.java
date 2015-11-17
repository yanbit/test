package com.dsk.utils;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author yanbit
 * @date Nov 17, 2015 1:20:13 PM
 * @todo TODO
 */
public class DBPool {
  private static DBPool dbPool;
  private ComboPooledDataSource dataSource;

  static {
    dbPool = new DBPool();
  }

  public DBPool() {
    try {
      dataSource = new ComboPooledDataSource();
      // dataSource.setUser("id");
      // dataSource.setPassword("pw");
      dataSource.setJdbcUrl("jdbc:hive2://10.1.3.59:21050/;auth=noSasl");
      dataSource.setDriverClass("org.apache.hive.jdbc.HiveDriver");
      dataSource.setInitialPoolSize(5);
      dataSource.setMinPoolSize(5);
      dataSource.setMaxPoolSize(10);
      dataSource.setMaxStatements(50);
      dataSource.setMaxIdleTime(60);
    } catch (PropertyVetoException e) {
      throw new RuntimeException(e);
    }
  }

  public final static DBPool getInstance() {
    return dbPool;
  }

  public final Connection getConnection() {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new RuntimeException("无法从数据源获取连接", e);
    }
  }

  public static void main(String[] args) throws SQLException {
    Connection con = null;
    try {
      con = DBPool.getInstance().getConnection();
    } catch (Exception e) {
    } finally {
      if (con != null)
        con.close();
    }
  }

}
