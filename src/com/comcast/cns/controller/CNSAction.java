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

import com.comcast.cmb.common.controller.Action;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.util.Util;
import javax.servlet.http.HttpServletRequest;

/**
 * Abstract class representing all actions we accept for CNS
 *
 * @author bwolf
 */
public abstract class CNSAction extends Action {

  public CNSAction(String actionName) {
    super(actionName);
  }

  @Override
  public boolean isActionAllowed(
      User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {

    String topicArn = request.getParameter("TopicArn");

    if (topicArn == null && request.getParameter("SubscriptionArn") != null) {

      CNSSubscription subscription =
          PersistenceFactory.getSubscriptionPersistence()
              .getSubscription(request.getParameter("SubscriptionArn"));

      if (subscription != null) {
        topicArn = subscription.getTopicArn();
      }
    }

    if (user.getUserId().equals(Util.getUserIdFromTopicArn(topicArn))) {
      return true;
    }

    if (policy == null) {
      return false;
    }

    return policy.isAllowed(user, service + ":" + this.actionName);
  }
}
