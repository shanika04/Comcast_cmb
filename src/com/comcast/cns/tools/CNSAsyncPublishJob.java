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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cns.io.EndpointAsyncPublisherWrapper;
import com.comcast.cns.io.EndpointPublisherFactory;
import com.comcast.cns.io.HTTPEndpointAsyncPublisher;
import com.comcast.cns.io.IEndpointPublisher;
import com.comcast.cns.io.IPublisherCallback;
import com.comcast.cns.model.CNSMessage;
import com.comcast.cns.model.CNSRetryPolicy;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSSubscriptionDeliveryPolicy;
import com.comcast.cns.persistence.ICNSAttributesPersistence;
import com.comcast.cns.persistence.SubscriberNotFoundException;
import com.comcast.cns.util.Util;
import com.comcast.cqs.model.CQSMessageAttribute;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

/**
 * Helper class representing individual (to one endpoint) publish job
 *
 * <p>Class is thread-safe
 */
public class CNSAsyncPublishJob implements Runnable, IPublisherCallback {

  private static Logger logger = Logger.getLogger(CNSAsyncPublishJob.class);

  enum RetryPhase {
    None,
    ImmediateRetry,
    PreBackoff,
    Backoff,
    PostBackoff;
  }

  private final CNSMessage message;
  private final User user;
  private final CnsSubscriptionProtocol protocol;
  private final String endpoint;
  private final String subArn;
  private final String queueUrl;
  private final String receiptHandle;
  private final AtomicInteger endpointPublishJobCount;
  private final boolean rawDelivery;
  private final Map<String, CQSMessageAttribute> messageAttributes;

  private IEndpointPublisher publisher;

  public volatile int numRetries = 0;
  private volatile int maxDelayRetries = 0;

  public String getSubscriptionArn() {
    return subArn;
  }

  private void letMessageDieForEndpoint() {

    // decrement the total number of sub-tasks and if counter is 0 delete publish job

    if (endpointPublishJobCount.decrementAndGet() == 0) {
      try {
        CQSHandler.deleteMessage(queueUrl, receiptHandle);
        logger.debug(
            "event=deleting_publish_job_from_cqs message_id="
                + message.getMessageId()
                + " queue_url="
                + queueUrl
                + " receipt_handle="
                + receiptHandle);
      } catch (Exception ex) {
        logger.error("event=failed_to_kill_message", ex);
      }
    }
  }

  /** Call this method only in retry mode for a single sub */
  public void doRetry() {

    try {

      ICNSAttributesPersistence attributePers = PersistenceFactory.getCNSAttributePersistence();
      CNSSubscriptionAttributes subAttr = attributePers.getSubscriptionAttributes(subArn);

      if (subAttr == null) {
        throw new CMBException(
            CMBErrorCodes.InternalError,
            "Could not get subscription delivery policy for subscripiton " + subArn);
      }

      CNSSubscriptionDeliveryPolicy deliveryPolicy = subAttr.getEffectiveDeliveryPolicy();
      CNSRetryPolicy retryPolicy = deliveryPolicy.getHealthyRetryPolicy();

      logger.debug("retry_policy=" + retryPolicy + "sub_arn=" + subArn);

      while (numRetries < retryPolicy.getNumNoDelayRetries()) {

        logger.debug("event=immediate_retry num_retries=" + numRetries);

        // handle immediate retry phase

        try {
          numRetries++;
          runCommon();
          return; // suceeded.
        } catch (Exception e) {
          logger.debug(
              "event=retry_failed phase="
                  + RetryPhase.ImmediateRetry.name()
                  + " attempt="
                  + numRetries);
        }
      }

      // handle pre-backoff phase

      if (numRetries < retryPolicy.getNumMinDelayRetries() + retryPolicy.getNumNoDelayRetries()) {

        logger.debug(
            "event=pre_backoff num_retries="
                + numRetries
                + " mind_delay_target_secs="
                + retryPolicy.getMinDelayTarget());
        numRetries++;

        CNSEndpointPublisherJobConsumer.submitForReDelivery(
            this, retryPolicy.getMinDelayTarget(), TimeUnit.SECONDS);

        // add 6 second buffer to avoid race condition (assuming we are enforcing a 5 sec http
        // timeout)

        CQSHandler.changeMessageVisibility(
            queueUrl, receiptHandle, retryPolicy.getMinDelayTarget() + 6);

        return;
      }

      // if reached here, in the backoff phase

      if (numRetries
          < retryPolicy.getNumRetries()
              - (retryPolicy.getNumMinDelayRetries()
                  + retryPolicy.getNumNoDelayRetries()
                  + retryPolicy.getNumMaxDelayRetries())) {

        numRetries++;

        int delay =
            Util.getNextRetryDelay(
                numRetries
                    - retryPolicy.getNumMinDelayRetries()
                    - retryPolicy.getNumNoDelayRetries(),
                retryPolicy.getNumRetries()
                    - retryPolicy.getNumMinDelayRetries()
                    - retryPolicy.getNumNoDelayRetries(),
                retryPolicy.getMinDelayTarget(),
                retryPolicy.getMaxDelayTarget(),
                retryPolicy.getBackOffFunction());

        logger.debug(
            "event=retry_notification phase="
                + RetryPhase.Backoff.name()
                + " delay="
                + delay
                + " attempt="
                + numRetries
                + " backoff_function="
                + retryPolicy.getBackOffFunction().name());

        CNSEndpointPublisherJobConsumer.submitForReDelivery(this, delay, TimeUnit.SECONDS);

        // add 6 second buffer to avoid race condition (assuming we are enforcing a 6 sec http
        // timeout)

        CQSHandler.changeMessageVisibility(queueUrl, receiptHandle, delay + 6);

        return;
      }

      if (numRetries < retryPolicy.getNumRetries()) { // remainder must be post-backoff

        logger.debug(
            "event=post_backoff max_delay_retries="
                + maxDelayRetries
                + " max_delay_target="
                + retryPolicy.getMaxDelayTarget());
        maxDelayRetries++;

        CNSEndpointPublisherJobConsumer.submitForReDelivery(
            this, retryPolicy.getMaxDelayTarget(), TimeUnit.SECONDS);

        // add 6 second buffer to avoid race condition (assuming we are enforcing a 5 sec http
        // timeout)

        CQSHandler.changeMessageVisibility(
            queueUrl, receiptHandle, retryPolicy.getMaxDelayTarget() + 6);

        return;
      }

      logger.debug(
          "event=retries_exhausted action=skip_message endpoint="
              + endpoint
              + " message="
              + message);

      letMessageDieForEndpoint();

    } catch (SubscriberNotFoundException e) {
      logger.error("event=retry_error error_code=subscriber_not_found sub_arn=" + subArn);
    } catch (Exception e) {
      logger.error(
          "event=retry_error endpoint="
              + endpoint
              + " protocol="
              + protocol
              + " message_length="
              + message.getMessage().length()
              + (user == null ? "" : " " + user),
          e);
    }
  }

  /**
   * Send the notification and let any exceptions bubble up
   *
   * @param publisher
   * @param protocol
   * @param endpoint
   * @param subArn
   * @throws Exception
   */
  private void runCommon() throws Exception {

    publisher.setEndpoint(endpoint);
    publisher.setMessage(message);
    publisher.setSubject(message.getSubject());
    publisher.setUser(user);
    publisher.setRawMessageDelivery(rawDelivery);
    publisher.send(); // this will most likely not fail because we call asynchronously
  }

  private void runCommonAndRetry() {

    try {

      int badResponseCounter = CNSEndpointPublisherJobConsumer.getNumBadResponses(endpoint);

      int failureSuspensionThreshold =
          CMBProperties.getInstance().getEndpointFailureCountToSuspensionThreshold();

      // only throw away new messages for bad endpoints (not those that are already in redlivery) -
      // in effect we are temporarily suspending
      // endpoints with more than three failures in the last minute

      if (failureSuspensionThreshold != 0
          && badResponseCounter > failureSuspensionThreshold
          && numRetries == 0) {

        logger.warn(
            "event=throwing_message_away reason=endpoint_has_too_many_bad_responses_in_last_minute endpoint="
                + endpoint
                + " bad_response_counter="
                + badResponseCounter
                + " attempt="
                + numRetries);

        letMessageDieForEndpoint();

      } else {
        runCommon();
      }

    } catch (Exception ex) {
      onFailure(1); // signal unknown error with status 1
    }
  }

  public CNSAsyncPublishJob(
      CNSMessage message,
      User user,
      CnsSubscriptionProtocol protocol,
      String endpoint,
      String subArn,
      boolean rawDelivery,
      String queueUrl,
      String receiptHandle,
      AtomicInteger endpointPublishJobCount,
      Map<String, CQSMessageAttribute> messageAttributes) {

    this.message = message;
    this.user = user;
    this.protocol = protocol;
    this.endpoint = endpoint;
    this.subArn = subArn;
    this.queueUrl = queueUrl;
    this.receiptHandle = receiptHandle;
    this.endpointPublishJobCount = endpointPublishJobCount;
    this.rawDelivery = rawDelivery;
    this.messageAttributes = messageAttributes;
  }

  @Override
  public void run() {

    // long ts1 = System.currentTimeMillis();
    // CMBControllerServlet.valueAccumulator.initializeAllCounters();

    if (protocol == CnsSubscriptionProtocol.http || protocol == CnsSubscriptionProtocol.https) {
      publisher = new HTTPEndpointAsyncPublisher(this);
    } else {
      publisher =
          new EndpointAsyncPublisherWrapper(
              this, EndpointPublisherFactory.getPublisherInstance(protocol));
    }

    runCommonAndRetry();

    // long ts2 = System.currentTimeMillis();
    // logger.debug("event=metrics endpoint=" + endpoint + " protocol=" + protocol.name() + "
    // message_length=" + message.getMessage().length() + (user == null ?"":" " +
    // user.getUserName()) + " responseTimeMS=" + (ts2 - ts1) + " CassandraTimeMS=" +
    // CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CassandraTime) + "
    // publishTimeMS=" +
    // CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSPublishSendTime) + "
    // CNSCQSTimeMS=" +
    // CMBControllerServlet.valueAccumulator.getCounter(AccumulatorName.CNSCQSTime));
    // CMBControllerServlet.valueAccumulator.deleteAllCounters();
  }

  @Override
  public void onSuccess() {

    logger.debug(
        "event=successful_delivery protocol="
            + protocol
            + " endpoint="
            + endpoint
            + " sub_arn="
            + subArn
            + " attempt="
            + numRetries);

    letMessageDieForEndpoint();

    CNSWorkerMonitor.getInstance().registerSendsRemaining(message.getMessageId(), -1);
    CNSWorkerMonitor.getInstance().registerBadEndpoint(endpoint, 0, 1, message.getTopicArn());
    CNSWorkerMonitor.getInstance().registerPublishMessage();
  }

  @Override
  public void onFailure(int status) {

    CNSEndpointPublisherJobConsumer.addBadResponseEvent(endpoint);

    if (protocol == CnsSubscriptionProtocol.http || protocol == CnsSubscriptionProtocol.https) {

      if (CNSEndpointPublisherJobConsumer.acceptableHttpResponseCodes.contains(status + "")) {

        // if posting fails with an acceptable http error code let it die

        logger.warn(
            "event=failed_to_deliver_message action=skip_message status_code="
                + status
                + " endpoint="
                + endpoint);

        letMessageDieForEndpoint();

      } else {

        // if posting fails with an unacceptable http error code register bad endpoint in rolling
        // window of 60 sec and start retry process

        logger.warn(
            "event=failed_to_deliver_message action=retry status_code="
                + status
                + " endpoint="
                + endpoint);

        CNSWorkerMonitor.getInstance().registerBadEndpoint(endpoint, 1, 1, message.getTopicArn());
        doRetry();
      }

    } else {

      // if sending fails for a non-http endpoint let it die immediately without retry

      logger.warn("event=failed_to_deliver_message action=skip_message endpoint=" + endpoint);

      letMessageDieForEndpoint();
    }
  }

  @Override
  public void onExpire() {
    onFailure(0); // signal expire with status 0
  }
}
