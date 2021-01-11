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
package com.comcast.cns.tools;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.RollingWindowCapture;
import com.comcast.cns.io.HTTPEndpointSyncPublisher;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
import org.jfree.util.Log;

/**
 * The implementation of monitoring for CNS.
 *
 * @author aseem Class is thread-safe
 */
public class CNSWorkerMonitor implements CNSWorkerMonitorMBean {

  private static Logger logger = Logger.getLogger(CNSWorkerMonitorMBean.class);
  private static final CNSWorkerMonitor Inst = new CNSWorkerMonitor();

  private CNSWorkerMonitor() {}

  public static CNSWorkerMonitor getInstance() {
    return Inst;
  }

  public ConcurrentHashMap<String, AtomicInteger> messageIdToSendRemaining =
      new ConcurrentHashMap<String, AtomicInteger>();

  private class BadEndpointInfo {
    AtomicInteger errors = new AtomicInteger();
    AtomicInteger tries = new AtomicInteger();
    ConcurrentHashMap<String, Boolean> topics = new ConcurrentHashMap<String, Boolean>();
  }

  private ConcurrentHashMap<String, BadEndpointInfo> badEndpoints =
      new ConcurrentHashMap<String, CNSWorkerMonitor.BadEndpointInfo>();
  private boolean cqsServiceAvailable = true;

  private class GenericEvent extends RollingWindowCapture.PayLoad {
    final AtomicInteger num = new AtomicInteger(1);
    final long timeStamp = System.currentTimeMillis();
  }

  private class CountMessagesVisitor implements RollingWindowCapture.Visitor<GenericEvent> {
    int num = 0;

    public void processNode(GenericEvent n) {
      num += n.num.get();
    }
  }

  private RollingWindowCapture<GenericEvent> publishMsgRW =
      new RollingWindowCapture<GenericEvent>(
          CMBProperties.getInstance().getRollingWindowTimeSec(), 10000);

  @Override
  public int getRecentNumberOfPublishedMessages() {
    CountMessagesVisitor c = new CountMessagesVisitor();
    publishMsgRW.visitAllNodes(c);
    return c.num;
  }

  @Override
  public int getPublishHttpPoolSize() {
    return HTTPEndpointSyncPublisher.getNumConnectionsInPool();
  }

  public void registerPublishMessage() {
    GenericEvent currentHead = publishMsgRW.getLatestPayload();
    if (currentHead == null || System.currentTimeMillis() - currentHead.timeStamp > 60000L) {
      publishMsgRW.addNow(new GenericEvent());
    } else {
      currentHead.num.incrementAndGet();
    }
  }

  public void registerSendsRemaining(String messageId, int remaining) {
    AtomicInteger newVal = new AtomicInteger();
    AtomicInteger oldVal = messageIdToSendRemaining.putIfAbsent(messageId, newVal);
    if (oldVal != null) {
      newVal = oldVal;
    }
    newVal.addAndGet(remaining);
    if (newVal.intValue() == 0) {
      messageIdToSendRemaining.remove(messageId);
    }
  }

  @Override
  public int getPendingPublishJobs() {
    int numNonZero = 0;
    for (Map.Entry<String, AtomicInteger> entry : messageIdToSendRemaining.entrySet()) {
      if (entry.getValue().get() != 0) {
        numNonZero++;
      }
    }
    return numNonZero;
  }

  @Override
  public boolean getCNSWorkerStatus() {
    return CMBProperties.getInstance().isCNSPublisherEnabled();
  }

  @Override
  public void startCNSWorkers() throws Exception {
    if (CMBProperties.getInstance().isCNSPublisherEnabled()) {
      return;
    }
    CMBProperties.getInstance().setCNSPublisherEnabled(true);
    try {
      CNSPublisher.start(CMBProperties.getInstance().getCNSPublisherMode());
    } catch (Exception e) {
      logger.error("event=start_cnsworker_error exception=" + e);
      throw e;
    }
  }

  @Override
  public void stopCNSWorkers() {
    CMBProperties.getInstance().setCNSPublisherEnabled(false);
  }

  public void clearBadEndpointsState() {
    badEndpoints.clear();
  }

  /**
   * Only capture stats for endpoints that have previous errors.
   *
   * @param endpoint
   * @param errors
   * @param numTries
   * @param topic
   */
  public void registerBadEndpoint(String endpoint, int errors, int numTries, String topic) {

    if (!badEndpoints.containsKey(endpoint) && errors == 0) {
      return; // no errors for this endpoint. Its good. return
    }

    BadEndpointInfo newVal = new BadEndpointInfo();
    BadEndpointInfo oldVal = badEndpoints.putIfAbsent(endpoint, newVal);

    if (oldVal != null) {
      newVal = oldVal;
    }

    newVal.errors.addAndGet(errors);
    newVal.tries.addAndGet(numTries);
    newVal.topics.putIfAbsent(topic, Boolean.TRUE);
  }

  /** @return map of endpointURL -> failure-rate, numTries, List<Topics> */
  @Override
  public Map<String, String> getErrorRateForEndpoints() {

    HashMap<String, String> ret = new HashMap<String, String>();

    for (Map.Entry<String, BadEndpointInfo> entry : badEndpoints.entrySet()) {

      String endpoint = entry.getKey();
      BadEndpointInfo val = entry.getValue();
      StringBuffer sb = new StringBuffer();
      int failureRate =
          (val.tries.get() > 0)
              ? (int) (((float) val.errors.get() / (float) val.tries.get()) * 100)
              : 0;
      sb.append("failureRate=")
          .append(failureRate)
          .append(",numTries=")
          .append(val.tries.get())
          .append(",topics=");
      Enumeration<String> topics = val.topics.keys();

      if (topics.hasMoreElements()) {
        sb.append(topics.nextElement());
      }

      while (topics.hasMoreElements()) {
        sb.append(",").append(topics.nextElement());
      }

      ret.put(endpoint, sb.toString());
    }

    return ret;
  }

  @Override
  public Map<String, Integer> getRecentErrorCountForEndpoints() {
    return CNSEndpointPublisherJobConsumer.getBadResponseCounts();
  }

  @Override
  public int getDeliveryQueueSize() {
    return CNSEndpointPublisherJobConsumer.MonitoringInterface.getDeliveryHandlersQueueSize();
  }

  @Override
  public int getRedeliveryQueueSize() {
    return CNSEndpointPublisherJobConsumer.MonitoringInterface.getReDeliveryHandlersQueueSize();
  }

  @Override
  public boolean isConsumerOverloaded() {
    return CNSEndpointPublisherJobConsumer.isOverloaded();
  }

  @Override
  public boolean clearWorkerQueues() {

    try {
      CNSPublisher.clearQueues();
      clearBadEndpointsState();
      return true;
    } catch (PersistenceException ex) {
      Log.error("event=clear_queue_failure", ex);
    }

    return false;
  }

  @Override
  public boolean isCQSServiceAvailable() {
    return cqsServiceAvailable;
  }

  public void registerCQSServiceAvailable(boolean available) {
    cqsServiceAvailable = available;
  }
}
