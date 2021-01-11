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

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.model.UserAuthModule;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.CMBProperties.IO_MODE;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.RollingWindowCapture;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import com.comcast.cns.model.CNSEndpointPublishJob;
import com.comcast.cns.model.CNSEndpointPublishJob.CNSEndpointSubscriptionInfo;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.persistence.CNSCachedEndpointPublishJob;
import com.comcast.cns.persistence.TopicNotFoundException;
import com.comcast.cqs.model.CQSMessage;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

/**
 * This is the Endpoint Job Consumer. Its role is to dequeue CNSEndpointPublishJobs from CQS, send
 * the notifications and retry if needed This class uses two internal executors to send
 * notifications: one for normal notifications and one for retrys
 *
 * @author aseem, bwolf
 *     <p>Class is thread-safe
 */
public class CNSEndpointPublisherJobConsumer implements CNSPublisherPartitionRunnable {

  private static Logger logger = Logger.getLogger(CNSEndpointPublisherJobConsumer.class);

  private static final String CNS_CONSUMER_QUEUE_NAME_PREFIX =
      CMBProperties.getInstance().getCNSEndpointPublishQueueNamePrefix();

  private static volatile ScheduledThreadPoolExecutor deliveryHandlers = null;
  private static volatile ScheduledThreadPoolExecutor reDeliveryHandlers = null;

  private static final RollingWindowCapture<BadEndpointEvent> badEndpointCounterWindow =
      new RollingWindowCapture<BadEndpointEvent>(60, 10000);

  public static final List<String> acceptableHttpResponseCodes =
      CMBProperties.getInstance().getCNSPublisherAcceptableHttpStatusCodes();

  private long processingDelayMillis = 10;

  public static class BadEndpointEvent extends RollingWindowCapture.PayLoad {
    public final String endpointUrl;

    public BadEndpointEvent(String endpointUrl) {
      this.endpointUrl = endpointUrl;
    }
  }

  private static class CountBadEndpointVisitor
      implements RollingWindowCapture.Visitor<BadEndpointEvent> {
    public final Map<String, Integer> badEndpointCounts = new HashMap<String, Integer>();

    public void processNode(BadEndpointEvent e) {
      if (!badEndpointCounts.containsKey(e.endpointUrl)) {
        badEndpointCounts.put(e.endpointUrl, 1);
      } else {
        badEndpointCounts.put(e.endpointUrl, badEndpointCounts.get(e.endpointUrl) + 1);
      }
    }
  }

  public static void addBadResponseEvent(String endpointUrl) {
    badEndpointCounterWindow.addNow(new BadEndpointEvent(endpointUrl));
  }

  public static int getNumBadResponses(String endpointUrl) {
    CountBadEndpointVisitor srv = new CountBadEndpointVisitor();
    badEndpointCounterWindow.visitAllNodes(srv);
    if (!srv.badEndpointCounts.containsKey(endpointUrl)) {
      return 0;
    }
    return srv.badEndpointCounts.get(endpointUrl);
  }

  public static Map<String, Integer> getBadResponseCounts() {
    CountBadEndpointVisitor srv = new CountBadEndpointVisitor();
    badEndpointCounterWindow.visitAllNodes(srv);
    return srv.badEndpointCounts;
  }

  private static volatile boolean initialized = false;

  private static volatile Integer testQueueLimit = null;

  public static class MonitoringInterface {

    public static int getDeliveryHandlersQueueSize() {
      return deliveryHandlers.getQueue().size();
    }

    public static int getReDeliveryHandlersQueueSize() {
      return reDeliveryHandlers.getQueue().size();
    }
  }

  public static void submitForReDelivery(Runnable job, long delay, TimeUnit unit) {
    reDeliveryHandlers.schedule(job, delay, unit);
  }

  /**
   * Make the appr executors Read the deliveryNumHandlers and the retryDeliveryNumHandlers
   * properties at startup Read the EndpointPublishQ_<m> property and ensuring they exist (create if
   * not)
   *
   * @throws PersistenceException
   */
  public static void initialize() throws PersistenceException {
    if (initialized) {
      return;
    }
    deliveryHandlers =
        new ScheduledThreadPoolExecutor(
            CMBProperties.getInstance().getCNSNumPublisherDeliveryHandlers());
    reDeliveryHandlers =
        new ScheduledThreadPoolExecutor(
            CMBProperties.getInstance().getCNSNumPublisherReDeliveryHandlers());
    logger.info("event=initializing_cns_consumer");
    initialized = true;
  }

  /**
   * shutsdown all internal thread-pools. Cannot use object again unless initialize() called again.
   */
  public static void shutdown() {
    deliveryHandlers.shutdownNow();
    reDeliveryHandlers.shutdownNow();
    initialized = false;
    logger.info("event=shutdown_cns_consumer");
  }

  /**
   * Server is overloaded if - the size of queue for deliveryHandlers is larger than
   * deliveryHandlerQLimit OR - The size of the reDeliveryHandlers is larger than
   * reDeliveryHandlerQLimit
   *
   * @return true if overloaded
   */
  public static boolean isOverloaded() {

    if (testQueueLimit == null) {

      if (deliveryHandlers.getQueue().size()
              >= CMBProperties.getInstance().getCNSDeliveryHandlerJobQueueLimit()
          || reDeliveryHandlers.getQueue().size()
              >= CMBProperties.getInstance().getCNSReDeliveryHandlerJobQueueLimit()) {
        return true;
      }

      return false;

    } else {

      logger.debug("event=is_overloaded queue_size=" + deliveryHandlers.getQueue().size());

      if (deliveryHandlers.getQueue().size() >= testQueueLimit
          || reDeliveryHandlers.getQueue().size() >= testQueueLimit) {
        return true;
      }

      return false;
    }
  }

  /**
   * a. Check if server is overloaded, if it is, go to sleep and retry c. Start polling the
   * EndpointPublishQ_<m> queues in a round-robin manner, getting the list of endpoints and handing
   * them to the delivery executor framework. d. Create another executor called the
   * reDeliveryExecutor that gets the failed endpoints from ( c). e. Keep in-memory structure that
   * maps all individual executor jobs to the main EndpointPublish job. Once all individual jobs
   * have completed, delete the EndpointPublish Job. f. Set HTTP and TCP handshake timeouts
   *
   * <p>Note: the method backs off exponentially by putting the thread to sleep.
   *
   * @return true if messages were found in current partition, false otherwise
   */
  @Override
  public boolean run(int partition) {

    boolean messageFound = false;
    long ts0 = System.currentTimeMillis();
    CMBControllerServlet.valueAccumulator.initializeAllCounters();

    if (!initialized) {
      throw new IllegalStateException("Not initialized");
    }

    try {

      long ts1 = System.currentTimeMillis();

      if (CNSPublisher.lastConsumerMinute.getAndSet(ts1 / (1000 * 60)) != ts1 / (1000 * 60)) {

        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        logger.info("event=ping version=" + CMBControllerServlet.VERSION + " ip=" + hostAddress);

        try {
          Map<String, String> values = new HashMap<String, String>();
          values.put("consumerTimestamp", System.currentTimeMillis() + "");
          values.put("jmxport", System.getProperty("com.sun.management.jmxremote.port", "0"));
          values.put("mode", CNSPublisher.getModeString());
          values.put("dataCenter", CMBProperties.getInstance().getCMBDataCenter());
          CNSPublisher.cassandraHandler.insertRow(
              CMBProperties.getInstance().getCNSKeyspace(),
              hostAddress,
              "CNSWorkers",
              values,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER,
              null);
        } catch (Exception ex) {
          logger.warn("event=ping_glitch", ex);
        }
      }

      if (isOverloaded()) {

        logger.info("event=cns_consumer_overload");

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          logger.error("event=run", e);
        }

        CMBControllerServlet.valueAccumulator.deleteAllCounters();
        return messageFound;
      }

      String queueName = CNS_CONSUMER_QUEUE_NAME_PREFIX + partition;
      String queueUrl = CQSHandler.getRelativeCnsInternalQueueUrl(queueName);
      CQSMessage msg = null;
      int waitTimeSecs = 0;

      if (CMBProperties.getInstance().isCQSLongPollEnabled()) {
        waitTimeSecs = CMBProperties.getInstance().getCMBRequestTimeoutSec();
      }

      List<CQSMessage> l = CQSHandler.receiveMessage(queueUrl, waitTimeSecs, 1);

      if (l.size() > 0) {
        msg = l.get(0);
      }

      CNSWorkerMonitor.getInstance().registerCQSServiceAvailable(true);

      if (msg != null) {

        // if long polling disabled and message found reset exponential backoff

        if (!CMBProperties.getInstance().isCQSLongPollEnabled()) {
          processingDelayMillis = 10;
        }

        messageFound = true;

        try {

          CNSEndpointPublishJob endpointPublishJob =
              (CMBProperties.getInstance().isCNSUseSubInfoCache())
                  ? CNSCachedEndpointPublishJob.parseInstance(msg.getBody())
                  : CNSEndpointPublishJob.parseInstance(msg.getBody());
          logger.debug("endpoint_publish_job=" + endpointPublishJob.toString());
          User pubUser =
              (new UserAuthModule()).getUserByUserId(endpointPublishJob.getMessage().getUserId());
          List<? extends CNSEndpointSubscriptionInfo> subs = endpointPublishJob.getSubInfos();

          CNSWorkerMonitor.getInstance()
              .registerSendsRemaining(endpointPublishJob.getMessage().getMessageId(), subs.size());

          AtomicInteger endpointPublishJobCount = new AtomicInteger(subs.size());

          for (CNSEndpointSubscriptionInfo sub : subs) {

            Runnable publishJob = null;
            CNSMessage message = endpointPublishJob.getMessage();
            message.setSubscriptionArn(sub.subArn);

            if (CMBProperties.getInstance().getCNSIOMode() == IO_MODE.SYNC) {
              publishJob =
                  new CNSPublishJob(
                      message,
                      pubUser,
                      sub.protocol,
                      sub.endpoint,
                      sub.subArn,
                      sub.rawDelivery,
                      queueUrl,
                      msg.getReceiptHandle(),
                      endpointPublishJobCount,
                      msg.getMessageAttributes());
            } else {
              publishJob =
                  new CNSAsyncPublishJob(
                      message,
                      pubUser,
                      sub.protocol,
                      sub.endpoint,
                      sub.subArn,
                      sub.rawDelivery,
                      queueUrl,
                      msg.getReceiptHandle(),
                      endpointPublishJobCount,
                      msg.getMessageAttributes());
            }

            deliveryHandlers.submit(publishJob);
          }

        } catch (TopicNotFoundException e) {
          logger.error("event=topic_not_found action=skip_job");
          CQSHandler.deleteMessage(queueUrl, msg.getReceiptHandle());
        } catch (Exception e) {
          logger.error("event=job_consumer_exception action=wait_for_revisibility", e);
        }

        long tsFinal = System.currentTimeMillis();
        logger.debug(
            "event=processed_consumer_job cns_cqs_ms="
                + CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime)
                + " resp_ms="
                + (tsFinal - ts0));

      } else {

        // if long polling disabled and no message found do exponential backoff

        if (!CMBProperties.getInstance().isCQSLongPollEnabled()) {

          if (processingDelayMillis
              < CMBProperties.getInstance().getCNSProducerProcessingMaxDelay()) {
            processingDelayMillis *= 2;
          }

          logger.debug("sleeping_ms=" + processingDelayMillis);

          Thread.sleep(processingDelayMillis);
        }
      }

    } catch (Exception ex) {
      logger.error("event=job_consumer_failure", ex);
      CQSHandler.ensureQueuesExist(
          CNS_CONSUMER_QUEUE_NAME_PREFIX,
          CMBProperties.getInstance().getCNSNumEndpointPublishJobQueues());
    }

    CMBControllerServlet.valueAccumulator.deleteAllCounters();
    return messageFound;
  }
}
