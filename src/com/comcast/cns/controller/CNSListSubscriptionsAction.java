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

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSSubscriptionPopulator;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.persistence.SubscriberNotFoundException;
import java.util.List;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * List subscriptions
 *
 * @author bwolf
 */
public class CNSListSubscriptionsAction extends CNSAction {

  private static Logger logger = Logger.getLogger(CNSListSubscriptionsAction.class);

  public CNSListSubscriptionsAction() {
    super("ListSubscriptions");
  }

  /**
   * The method simply gets the information from the user and request to call listSubscriptions,
   * then we take response and generate an XML response and put it in the parameter response
   *
   * @param user the user for whom we are listing the subscription
   * @param asyncContext
   */
  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    String userId = user.getUserId();
    String nextToken = null;

    if (request.getParameter("NextToken") != null) {
      nextToken = request.getParameter("NextToken");
      logger.debug("event=cns_subscription_list next_token=" + nextToken + " userid=" + userId);
    } else {
      logger.debug("event=cns_subscription_list userid=" + userId);
    }

    List<CNSSubscription> subscriptions = null;

    try {
      subscriptions =
          PersistenceFactory.getSubscriptionPersistence()
              .listSubscriptions(nextToken, null, userId);
    } catch (SubscriberNotFoundException ex) {
      throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid parameter nextToken");
    }

    if (subscriptions.size() >= 100) {

      // Check if there are more
      nextToken = subscriptions.get(99).getArn();
      List<CNSSubscription> nextSubscriptions =
          PersistenceFactory.getSubscriptionPersistence()
              .listSubscriptions(nextToken, null, userId);

      if (nextSubscriptions.size() == 0) {
        nextToken = null;
      } else {
        logger.debug(
            "event=cns_subscriptions_listed event=next_token_created next_token="
                + nextToken
                + " userid="
                + userId);
      }
    }

    String out = CNSSubscriptionPopulator.getListSubscriptionResponse(subscriptions, nextToken);
    writeResponse(out, response);
    return true;
  }

  @Override
  public boolean isActionAllowed(
      User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    return true;
  }
}
