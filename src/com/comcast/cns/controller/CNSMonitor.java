/**
 * Copyright 2012 Comcast Corporation
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.cns.controller;

import com.comcast.cmb.common.controller.CMBControllerServlet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The implementation of monitoring for CNS.
 *
 * @author aseem, boris Class is thread-safe
 */
public class CNSMonitor implements CNSMonitorMBean {

  private static final CNSMonitor Inst = new CNSMonitor();

  private CNSMonitor() {}

  public static CNSMonitor getInstance() {
    return Inst;
  }

  @Override
  public Map<String, AtomicLong> getCallStats() {
    return CMBControllerServlet.callStats;
  }

  @Override
  public Map<String, AtomicLong> getCallFailureStats() {
    return CMBControllerServlet.callFailureStats;
  }

  @Override
  public void resetCallStats() {
    CMBControllerServlet.initStats();
  }
}
