/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.zeppelin.interpreter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.DAGScheduler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SnappyContext;
import org.apache.spark.sql.SnappySession;
import org.apache.zeppelin.jdbc.JDBCInterpreter;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.ZeppelinContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.mutable.HashSet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static org.apache.commons.lang.StringUtils.containsIgnoreCase;

/**
 * Snappydatasql interpreter used to connect snappydata cluster using jdbc for performing sql
 * queries
 */
public class SnappyDataSqlZeppelinInterpreter extends Interpreter {
  private static final String SHOW_APPROX_RESULTS_FIRST = "show-instant-results-first";
  static Map<String, ParagraphState> paragraphStateMap = new HashMap<String, ParagraphState>();
  private Logger logger = LoggerFactory.getLogger(SnappyDataSqlZeppelinInterpreter.class);
  private static final char NEWLINE = '\n';
  private static final ExecutorService exService = Executors.newSingleThreadExecutor();

  private static final String EMPTY_STRING = "";
  private static final String SEMI_COLON = ";";
  SparkContext sc = null;
  LoadingCache<String, SnappyContext> paragraphContextCache = null;


  public SnappyDataSqlZeppelinInterpreter(Properties property) {
    super(property);
  }


  private int maxResult;

  @Override
  public void open() {
    if (null != SnappyContext.globalSparkContext()) {
      sc = SnappyContext.globalSparkContext();
      sc.hadoopConfiguration().set(Constants.FS_S3A_CONNECTION_MAXIMUM,"1000");
    }
    this.maxResult = Integer.parseInt(getProperty("zeppelin.spark.maxResult"));

    if (null != getProperty(Constants.FS_S3A_ACCESS_KEY) && null != getProperty(Constants.FS_S3A_SECRET_KEY)) {
      sc.hadoopConfiguration().set(Constants.FS_S3A_IMPL, "org.apache.hadoop.fs.s3a.S3AFileSystem");
      sc.hadoopConfiguration().set(Constants.FS_S3A_ACCESS_KEY, getProperty(Constants.FS_S3A_ACCESS_KEY));
      sc.hadoopConfiguration().set(Constants.FS_S3A_SECRET_KEY, getProperty(Constants.FS_S3A_SECRET_KEY));
      if (null != getProperty(Constants.FS_S3A_CONNECTION_MAXIMUM)) {
        sc.hadoopConfiguration().set(Constants.FS_S3A_CONNECTION_MAXIMUM,
                getProperty(Constants.FS_S3A_CONNECTION_MAXIMUM));
      }
    }

    paragraphContextCache = CacheBuilder.newBuilder()
            .maximumSize(50)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build(
                    new CacheLoader<String, SnappyContext>() {
                      @Override
                      public SnappyContext load(String paragraphId) throws Exception {
                        return new SnappyContext(sc);
                      }
                    }
            );
  }

  private String getJobGroup(InterpreterContext context) {
    return "zeppelin-" + context.getParagraphId();
  }


  @Override
  public void close() {

  }

  public boolean concurrentSQL() {
    return Boolean.parseBoolean(getProperty("zeppelin.spark.concurrentSQL"));
  }


  private SnappyDataZeppelinInterpreter getSparkInterpreter() {
    LazyOpenInterpreter lazy = null;
    SnappyDataZeppelinInterpreter snappyDataZeppelinInterpreter = null;
    Interpreter p = getInterpreterInTheSameSessionByClassName(SnappyDataZeppelinInterpreter.class.getName());

    while (p instanceof WrappedInterpreter) {
      if (p instanceof LazyOpenInterpreter) {
        lazy = (LazyOpenInterpreter) p;
      }
      p = ((WrappedInterpreter) p).getInnerInterpreter();
    }
    snappyDataZeppelinInterpreter = (SnappyDataZeppelinInterpreter) p;

    if (lazy != null) {
      lazy.open();
    }
    return snappyDataZeppelinInterpreter;
  }


  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {


    if (concurrentSQL()) {
      sc.setLocalProperty("spark.scheduler.pool", "fair");
    } else {
      sc.setLocalProperty("spark.scheduler.pool", null);
    }

    sc.setJobGroup(getJobGroup(contextInterpreter), "Zeppelin", false);

    Thread.currentThread().setContextClassLoader(org.apache.spark.util.Utils.getSparkClassLoader());
    //SnappyContext snc = new SnappyContext(sc);
    String id = contextInterpreter.getParagraphId();
    SnappyContext snc = null;
    try {
      snc = paragraphContextCache.get(id);
      if (null != getProperty(Constants.SPARK_SQL_SHUFFLE_PARTITIONS)) {
        snc.setConf(Constants.SPARK_SQL_SHUFFLE_PARTITIONS,
                getProperty(Constants.SPARK_SQL_SHUFFLE_PARTITIONS));
      }
    } catch (ExecutionException e) {
      logger.error("Error initializing SnappyContext");
      e.printStackTrace();
    }

    cmd = cmd.trim();
    if (cmd.startsWith(SHOW_APPROX_RESULTS_FIRST)) {
      cmd = cmd.replaceFirst(SHOW_APPROX_RESULTS_FIRST, EMPTY_STRING);

      /**
       * As suggested by Jags and suyog
       * This will allow user to execute multiple queries in same paragraph.
       * But will return results of the last query.This is mainly done to
       * allow user to set properties for JDBC connection
       *
       */
      String queries[] = cmd.split(SEMI_COLON);
      for (int i = 0; i < queries.length - 1; i++) {
        InterpreterResult result = executeSql(snc, queries[i], contextInterpreter, false);
        if (result.code().equals(InterpreterResult.Code.ERROR)) {
          sc.clearJobGroup();
          return result;
        }
      }
      if (shouldExecuteApproxQuery(id)) {

        for (InterpreterContextRunner r : contextInterpreter.getRunners()) {
          if (id.equals(r.getParagraphId())) {

            String query = queries[queries.length - 1] + " with error";
            final InterpreterResult res = executeSql(snc, query, contextInterpreter, true);
            exService.submit(new QueryExecutor(r));
            sc.clearJobGroup();
            return res;
          }
        }

      } else {
        String query = queries[queries.length - 1];
        sc.clearJobGroup();
        return executeSql(snc, query, contextInterpreter, false);
      }
      return null;
    } else {
      String queries[] = cmd.split(SEMI_COLON);
      for (int i = 0; i < queries.length - 1; i++) {
        InterpreterResult result = executeSql(snc, queries[i], contextInterpreter, false);
        if (result.code().equals(InterpreterResult.Code.ERROR)) {
          sc.clearJobGroup();
          return result;
        }
      }
      sc.clearJobGroup();
      return executeSql(snc, queries[queries.length - 1], contextInterpreter, false);

    }

  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
            SnappyDataSqlZeppelinInterpreter.class.getName() + this.hashCode(), 10);
  }

  @Override
  public void cancel(InterpreterContext interpreterContext) {
    ParagraphState state = new ParagraphState();
    state.setIsCancelCalled(true);
    paragraphStateMap.put(interpreterContext.getParagraphId(), state);
    sc.cancelJobGroup(getJobGroup(interpreterContext));
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext interpreterContext) {
    SnappyDataZeppelinInterpreter snappyDataZeppelinInterpreter = getSparkInterpreter();
    return snappyDataZeppelinInterpreter.getProgress(interpreterContext);

  }

  /**
   * The content of this method are borrowed from JDBC interpreter of apache zeppelin
   *
   * @param snc
   * @param sql
   * @param interpreterContext
   * @return
   */
  private InterpreterResult executeSql(SnappyContext snc, String sql,
                                       InterpreterContext interpreterContext, boolean isApproxQuery) {

    String paragraphId = interpreterContext.getParagraphId();

    try {

      StringBuilder msg = new StringBuilder();

      long startTime = System.currentTimeMillis();
      Dataset ds = snc.sql(sql);
      String data = null;
      if (null != ds) {
        data = ZeppelinContext.showDF(snc.sparkContext(), interpreterContext, ds, maxResult);
      }
      long endTime = System.currentTimeMillis();

      data = data.trim();
      if (null != data && data != EMPTY_STRING && data.split("\n").length>1) {
        msg.append(data);
        msg.append(NEWLINE);


        if (isApproxQuery) {
          paragraphStateMap.get(paragraphId).setTimeRequiredForApproxQuery(endTime - startTime);
          msg.append("\n<font color=red>Time required to execute query on sample table : "
                  + (endTime - startTime) + " millis.Executing base query ...</font>");

        } else if (paragraphStateMap.containsKey(paragraphId)) {

          paragraphStateMap.get(paragraphId).setTimeRequiredForBaseQuery(endTime - startTime);
          msg.append("\n<font color=red>Time required to execute query on sample table : "
                  + paragraphStateMap.get(paragraphId).getTimeRequiredForApproxQuery() + " millis.</font><br>");
          msg.append("\n<font color=red>Time required to execute query on base table : "
                  + paragraphStateMap.get(paragraphId).getTimeRequiredForBaseQuery() + " millis.</font>");
          paragraphStateMap.remove(paragraphId);
        } else {
          msg.append("\n<br><font color=red>Time required to execute query : "
                  + (endTime - startTime) + " millis.</font>");

        }
      } else {
        // Response contains either an update count or there are no results.
      }


      return new InterpreterResult(InterpreterResult.Code.SUCCESS, msg.toString());

    } catch (Exception e) {
      if (!paragraphStateMap.containsKey(paragraphId) || !paragraphStateMap.get(paragraphId).isCancelCalled) {
        logger.error("Cannot run " + sql, e);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(e.getMessage()).append("\n");
        stringBuilder.append(e.getClass().toString()).append("\n");
        stringBuilder.append(StringUtils.join(e.getStackTrace(), "\n"));
        return new InterpreterResult(InterpreterResult.Code.ERROR, stringBuilder.toString());
      } else {
        paragraphStateMap.remove(paragraphId);
        // Don't show error in case of cancel
        return new InterpreterResult(InterpreterResult.Code.KEEP_PREVIOUS_RESULT, EMPTY_STRING);
      }
    }
  }


  /**
   * This method is needed in case of show-approx-results-first directive to toggle the state of paragraph
   *
   * @param paragraphId
   * @return
   */
  private boolean shouldExecuteApproxQuery(String paragraphId) {

    if (paragraphStateMap.containsKey(paragraphId) && !paragraphStateMap.get(paragraphId).isExecuteApproxQuery()) {
      //Toggle the flag for next execution
      paragraphStateMap.get(paragraphId).setExecuteApproxQuery(true);
      return false;

    } else {
      ParagraphState paragraphState = new ParagraphState();
      paragraphState.setExecuteApproxQuery(false);
      //Toggle the flag for next execution
      paragraphStateMap.put(paragraphId, paragraphState);
      return true;
    }


  }


  class QueryExecutor implements Callable<Integer> {

    InterpreterContextRunner runner;

    QueryExecutor(InterpreterContextRunner runner) {
      this.runner = runner;
    }

    @Override
    public Integer call() throws Exception {

      try {
              /*Adding a delay of few milliseconds in order for zeppelin to render
               the result.As this delay is after the query execution it will not
               be considered in query time. This delay is basically the gap between
               first query and spawning of the next query.
              */
        Thread.sleep(200);
      } catch (InterruptedException interruptedException) {

        //Ignore this exception as this should not harm the behaviour
      }
      runner.run();

      return 0;
    }
  }
}
