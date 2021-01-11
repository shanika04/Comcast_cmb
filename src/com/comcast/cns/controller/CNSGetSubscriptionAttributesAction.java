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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSAttributePopulator;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.util.CNSErrorCodes;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * Get Subsctiption Attributes
 *
 * @author bwolf, jorge
 */
public class CNSGetSubscriptionAttributesAction extends CNSAction {

  private static Logger logger = Logger.getLogger(CNSGetSubscriptionAttributesAction.class);

  public CNSGetSubscriptionAttributesAction() {
    super("GetSubscriptionAttributes");
  }

  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    String userId = user.getUserId();
    String subscriptionArn = request.getParameter("SubscriptionArn");

    if ((userId == null) || (subscriptionArn == null)) {
      logger.error(
          "event=cns_get_subscription_attributes error_code=InvalidParameters subscription_arn="
              + subscriptionArn
              + " user_id="
              + userId);
      throw new CMBException(CNSErrorCodes.CNS_InvalidParameter, "missing parameters");
    }

    CNSSubscriptionAttributes attr =
        PersistenceFactory.getCNSAttributePersistence().getSubscriptionAttributes(subscriptionArn);
    CNSSubscription sub =
        PersistenceFactory.getSubscriptionPersistence().getSubscription(subscriptionArn);
    String out = CNSAttributePopulator.getGetSubscriptionAttributesResponse(sub, attr);

    logger.debug(
        "event=cns_get_subscription_attributes subscription_arn="
            + subscriptionArn
            + " user_id="
            + userId);

    writeResponse(out, response);
    return true;
  }
}
