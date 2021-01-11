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
package com.comcast.cns.io;

import com.comcast.cns.model.CNSSubscription;
import org.apache.log4j.Logger;

/**
 * Factory to get appt publisher given protocol
 *
 * @author aseem, jorge, bwolf
 */
public class EndpointPublisherFactory {

  protected static Logger logger = Logger.getLogger(EndpointPublisherFactory.class);

  public static IEndpointPublisher getPublisherInstance(
      CNSSubscription.CnsSubscriptionProtocol protocol) {

    switch (protocol) {
      case https:
      case http:
        return new HTTPEndpointSyncPublisher();
      case email:
        return new EmailEndpointPublisher();
      case email_json:
        return new EmailJsonEndpointPublisher();
      case sqs:
        return new SQSEndpointPublisher();
      case cqs:
        return new CQSEndpointPublisher();
      case redis:
        return new RedisPubSubEndpointPublisher();
    }

    return null;
  }
}
