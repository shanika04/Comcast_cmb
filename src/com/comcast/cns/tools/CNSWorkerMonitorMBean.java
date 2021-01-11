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

import java.util.Map;

/**
 * Interface for monitoring CNS
 *
 * @author aseem
 */
public interface CNSWorkerMonitorMBean {

  /** @return total number of publishMessage() called across all topics in a rolling window */
  public int getRecentNumberOfPublishedMessages();

  /** @return number of http connections in the pool for HTTP publisher */
  public int getPublishHttpPoolSize();

  /**
   * @return total number of publish jobs for which at least one endpoint is not yet succesfully
   *     published but we are still trying
   */
  public int getPendingPublishJobs();

  /** @return map of endpointURL -> failure-rate-percentage, numTries, Topic1, Topic2,.. */
  public Map<String, String> getErrorRateForEndpoints();

  /** @return map of endpointURL -> number of failures over last 60 seconds */
  public Map<String, Integer> getRecentErrorCountForEndpoints();

  /** @return size of delivery handler queue */
  public int getDeliveryQueueSize();

  /** @return size of redelivery handler queue */
  public int getRedeliveryQueueSize();

  /** */
  public boolean isCQSServiceAvailable();

  /**
   * @return true if consumer is no longer taking new ep-publish-jobs off the queue because its too
   *     busy. false otherwise
   */
  public boolean isConsumerOverloaded();

  /** Clear all bad endpoint state */
  public void clearBadEndpointsState();

  /** Clear all worker queues and reinitialize (potentially useful when worker is overloaded) */
  public boolean clearWorkerQueues();

  /** @return return status of cns workers */
  public boolean getCNSWorkerStatus();

  /** Start CNS Workers */
  public void startCNSWorkers() throws Exception;

  /** Stop CNS Workers */
  public void stopCNSWorkers();
}
