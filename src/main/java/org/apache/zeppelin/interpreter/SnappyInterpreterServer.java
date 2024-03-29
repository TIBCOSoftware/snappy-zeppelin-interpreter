/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class SnappyInterpreterServer extends RemoteInterpreterServer {
  public static Logger logger = LoggerFactory.getLogger(SnappyInterpreterServer.class);

  public SnappyInterpreterServer(String callbackHost, int callbackPort,
                                 String portRange) throws IOException, TTransportException {
    this(callbackHost, callbackPort, portRange, false);
  }

  public SnappyInterpreterServer(String callbackHost, int callbackPort, String portRange,
                                 boolean isTest) throws TTransportException, IOException {
    super(callbackHost, callbackPort, portRange, isTest);
    logger.info("Initializing SnappyInterpreter Server");

    GemFireCacheImpl.getInstance().getResourceManager().
            addResourceListener(InternalResourceManager.ResourceType.ALL, new MemoryListenerImpl());

  }

  /**
   *Overriding the RemoteInterpreterServer's Shutdown method to avoid shutdown of the
   * Leadnode and RemoteInterpreter by zeppelin server
   */

  @Override
  public void shutdown() throws TException {
    SecurityManager securityManager = System.getSecurityManager();
    if (securityManager != null && securityManager.getClass().getName().contains("NoExitSecurityManager")) {
      super.shutdown();
    }
  }

  /**
   * This method is called from th Lead shutdown to clean stop the interpreter
   *
   * @param forceFully
   * @throws TException
   */
  public void shutdown(Boolean forceFully) throws TException {
    long startTime=System.currentTimeMillis();
    if (forceFully) {
      super.shutdown();
      logger.info("Shutting down SnappyInterpreterServer");
    }
  }
}
