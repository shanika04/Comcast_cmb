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
import com.comcast.cns.io.CNSTopicPopulator;
import com.comcast.cns.model.CNSTopic;
import java.util.List;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * List topics
 *
 * @author bwolf
 */
public class CNSListTopicsAction extends CNSAction {

  private static Logger logger = Logger.getLogger(CNSListTopicsAction.class);

  public CNSListTopicsAction() {
    super("ListTopics");
  }

  /**
   * Get all the topics for this user and output them in XML to the response
   *
   * @param user The user for which we are getting the list of topics
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
      logger.debug("event=cns_topic_list next_token=" + nextToken + " userid=" + userId);
    } else {
      logger.debug("event=cns_topic_list userid=" + userId);
    }

    List<CNSTopic> topics = PersistenceFactory.getTopicPersistence().listTopics(userId, nextToken);

    if (topics.size() >= 100) {

      nextToken = topics.get(99).getArn();
      List<CNSTopic> nextTopics =
          PersistenceFactory.getTopicPersistence().listTopics(userId, nextToken);

      if (nextTopics.size() == 0) {
        nextToken = null;
      } else {
        logger.debug(
            "event=cns_topic_listed event=next_token_created next_token="
                + nextToken
                + " user_id="
                + userId);
      }
    }

    String out = CNSTopicPopulator.getListTopicsResponse(topics, nextToken);
    writeResponse(out, response);
    return true;
  }

  @Override
  public boolean isActionAllowed(
      User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    return true;
  }
}
