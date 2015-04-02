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
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.util.FileUtil;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class TpcDsSingleTest {
  private static final Log LOG = LogFactory.getLog(TpcDsSingleTest.class);

  protected static TajoClient client;

  protected static List<String> successQueries = new ArrayList<String>();
  protected static long startTime;
  protected static long endTime;

  protected static int numTargetQueries;
  protected static String databaseName;
  protected static LocalTajoTestingUtility util;

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

  public void testAllQueries(boolean local) throws Exception {
    String resultParentPath = TpcDSTestUtil.getResultDataPath();
    File queryResultParentDir = null;
    if (local) {
      File resourceDir = null;
      if (resultParentPath == null || resultParentPath.isEmpty()) {
        resourceDir = new File(ClassLoader.getSystemResource("tpcds").getPath());
        queryResultParentDir = new File(resourceDir, "results");
      } else {
        queryResultParentDir = new File(resultParentPath);
      }
      if (!queryResultParentDir.exists()) {
        throw new Exception("Result directory not exists: " + resultParentPath);
      }
    } else {
      if (resultParentPath != null && !resultParentPath.isEmpty()) {
        queryResultParentDir = new File(resultParentPath);
        if (!queryResultParentDir.exists()) {
          throw new Exception("Result directory not exists: " + resultParentPath);
        }
      }
    }

    File queryRoot = new File(ClassLoader.getSystemResource("tpcds").getPath(), "/queries");
    //String[] queries = FileUtil.readTextFile(new File(queryRoot, "query.list")).split("\\n");
    String query = FileUtil.readTextFile(new File(queryRoot, getProperty("QUERY_PATH", true)));
    String queryId = getQueryId(getProperty("QUERY_PATH", true));

    numTargetQueries = 0;
    startTime = System.currentTimeMillis();
    try {
      long queryStartTime = System.currentTimeMillis();
      ResultSet res = client.executeQueryAndGetResult(query);
      try {
        String resultSetData = TpcDSTestUtil.resultSetToString(res);

        if (!local && queryResultParentDir == null) {
          System.out.println("==============================================");
          System.out.println(queryId + "result data:");
          System.out.println("----------------------------------------------");
          System.out.println(resultSetData);
          System.out.println("----------------------------------------------");
        } else {
          String expectedData = FileUtil.readTextFile(new File(queryResultParentDir, queryId + ".result"));

          if (!expectedData.equals(resultSetData)) {
            System.out.println("==============================================");
            System.out.println("expected data:");
            System.out.println("----------------------------------------------");
            System.out.println(expectedData);
            System.out.println("real data:");
            System.out.println("----------------------------------------------");
            System.out.println(resultSetData);
            System.out.println("----------------------------------------------");
          }
          assertEquals("TPC-DS Query: " + queryId + " failed", expectedData, resultSetData);
        }
      } finally {
        res.close();
      }

      String logMessage = queryId + " (" + (System.currentTimeMillis() - queryStartTime) / 1000 + " sec)";
      LOG.info("========> Query Finished: " + logMessage);
      successQueries.add(logMessage);
    } finally {
      endTime = System.currentTimeMillis();
    }
  }

  protected String getQueryId(String queryFileName) {
    String[] tokens = queryFileName.split("/");
    String queryName = tokens[tokens.length - 1];
    return queryName.split("\\.")[0];
  }
}
