/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import io.snappydata.gemxd.MemoryNotificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryListenerImpl implements MemoryNotificationListener {
  public static Logger logger = LoggerFactory.getLogger(MemoryListenerImpl.class);
  @Override
  public void notifyCriticalUp() {

    logger.warn("Critical Up memory event Occured");
    SnappyDataZeppelinInterpreter.cancelAllJobsAndPause();
  }

  @Override
  public void notifyEvictionUp() {

  }
}
