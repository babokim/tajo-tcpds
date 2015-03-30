/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamoin.tajo.tpcds;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.KeyValueSet;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TpcDSTestUtil {
  private static final Log LOG = LogFactory.getLog(TpcDSTestUtil.class);

  static String [] tableNames = {
      "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
      "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band",
      "inventory", "item", "promotion", "reason", "ship_mode",
      "store", "store_returns", "store_sales", "time_dim", "warehouse",
      "web_page", "web_returns", "web_sales", "web_site"
  };

  private static String getProperty(String name, boolean throwException) {
    String value = System.getProperty(name);
    if (value == null || value.isEmpty()) {
      value = System.getenv(name);
    }

    if (throwException && (value == null || value.isEmpty())) {
      throw new RuntimeException("No property [" + name + "] in conf/tajo-tpcds-env.sh");
    }

    return value;
  }

  public static String getDatabaseName() {
    return getProperty("DATABASE_NAME", true);
  }

  private static String getDataDir() {
    return getProperty("TPCDS_DATA_DIR", true);
  }

  public static String getTajoMaster() {
    return getProperty("TAJO_MASTER_CLIENT_ADDR", true);
  }


  public static String getResultDataPath() {
    return getProperty("RESULTS_DIR", false);
  }

  public static void createTables(String database, TajoClient client) throws Exception {
    String dataDir = getDataDir();
    if (dataDir == null || dataDir.isEmpty()) {
      throw new IOException("No TPCDS_DATA_DIR property. Use -DTPCDS_DATA_DIR=<data dir>");
    }

    if (dataDir.startsWith("hdfs://")) {
      Path path = new Path(dataDir);
      FileSystem fs = path.getFileSystem(new Configuration());
      for (String eachTable : tableNames) {
        Path tableDataDir = new Path(path, eachTable);
        if (!fs.exists(tableDataDir)) {
          throw new IOException(eachTable + " data dir [" + tableDataDir + "] not exists.");
        }
      }
    } else {
      File dataDirFile = new File(dataDir);
      if (!dataDirFile.exists()) {
        throw new IOException("TPCDS_DATA_DIR [" + dataDir + "] not exists.");
      }
      if (dataDirFile.isFile()) {
        throw new IOException("TPCDS_DATA_DIR [" + dataDir + "] is not a directory.");
      }

      for (String eachTable : tableNames) {
        File tableDataDir = new File(dataDirFile, eachTable);
        if (!tableDataDir.exists()) {
          throw new IOException(eachTable + " data dir [" + tableDataDir + "] not exists.");
        }
      }
    }

    KeyValueSet opt = new KeyValueSet();
    opt.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);

    LOG.info("Create database: " + database);
    client.executeQuery("create database if not exists " + database);

    Path tpcdsResourceURL = new Path(ClassLoader.getSystemResource("tpcds").toString());

    Path ddlPath = new Path(tpcdsResourceURL, "ddl");
    FileSystem localFs = FileSystem.getLocal(new Configuration());

    FileStatus[] files = localFs.listStatus(ddlPath);

    String dataDirWithPrefix = dataDir;
    if (dataDir.indexOf("://") < 0) {
      dataDirWithPrefix = "file://" + dataDir;
    }

    for (FileStatus eachFile: files) {
      if (eachFile.isFile()) {
        String tableName = eachFile.getPath().getName().split("\\.")[0];
        String query = FileUtil.readTextFile(new File(eachFile.getPath().toUri()));
        query = query.replace("${DB}", database);
        query = query.replace("${DATA_LOCATION}", dataDirWithPrefix + "/" + tableName);

        LOG.info("Create table:" + tableName + "," + query);
        client.executeQuery(query);
      }
    }
  }

  public static String resultSetToString(ResultSet resultSet) throws SQLException {
    StringBuilder sb = new StringBuilder();
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int numOfColumns = rsmd.getColumnCount();

    for (int i = 1; i <= numOfColumns; i++) {
      if (i > 1) sb.append(",");
      String columnName = rsmd.getColumnName(i);
      sb.append(columnName);
    }
    sb.append("\n-------------------------------\n");

    while (resultSet.next()) {
      for (int i = 1; i <= numOfColumns; i++) {
        if (i > 1) sb.append(",");
        String columnValue = resultSet.getString(i);
        if (resultSet.wasNull()) {
          columnValue = "null";
        }
        sb.append(columnValue);
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}

