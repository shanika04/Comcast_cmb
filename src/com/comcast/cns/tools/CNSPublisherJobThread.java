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
import org.apache.log4j.Logger;

/**
 * This class represents Thread that backoff when no message exists in all the partitions
 *
 * @author aseem
 */
public class CNSPublisherJobThread extends Thread {

  private static Logger logger = Logger.getLogger(CNSPublisherJobThread.class);

  private final CNSPublisherPartitionRunnable runnable;
  private final int partitionNumber;

  public CNSPublisherJobThread(
      String threadName, CNSPublisherPartitionRunnable runnable, int partitionNumber) {

    super(threadName);
    this.runnable = runnable;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public void run() {
    while (true) {
      runnable.run(this.partitionNumber);
      if (!CMBProperties.getInstance().isCNSPublisherEnabled()) {
        break;
      }
    }
  }
}
