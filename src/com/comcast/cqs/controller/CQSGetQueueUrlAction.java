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
package com.comcast.cqs.controller;

import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cqs.io.CQSQueuePopulator;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSErrorCodes;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Get queue url
 *
 * @author bwolf, vvenkatraman, baosen
 */
public class CQSGetQueueUrlAction extends CQSAction {

  public CQSGetQueueUrlAction() {
    super("GetQueueUrl");
  }

  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    String queueName = request.getParameter("QueueName");

    if (queueName == null) {
      throw new CMBException(CMBErrorCodes.MissingParameter, "Parameter QueueName not found");
    }

    CQSQueue queue = PersistenceFactory.getQueuePersistence().getQueue(user.getUserId(), queueName);

    if (queue == null) {
      throw new CMBException(
          CQSErrorCodes.NonExistentQueue,
          "Queue not found with name " + queueName + " for user " + user.getUserId());
    }

    String out = CQSQueuePopulator.getQueueUrlResponse(queue);
    writeResponse(out, response);

    return true;
  }

  @Override
  public boolean isActionAllowed(
      User user, HttpServletRequest request, String service, CMBPolicy policy) {
    return true;
  }
}
