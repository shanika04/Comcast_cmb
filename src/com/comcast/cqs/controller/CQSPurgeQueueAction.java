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

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSQueue;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Clear queue
 *
 * @author vvenkatraman, bwolf
 */
public class CQSPurgeQueueAction extends CQSAction {

  public CQSPurgeQueueAction() {
    super("PurgeQueue");
  }

  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    CQSQueue queue = CQSCache.getCachedQueue(user, request);

    for (int shard = 0; shard < queue.getNumberOfShards(); shard++) {
      PersistenceFactory.getCQSMessagePersistence().clearQueue(queue.getRelativeUrl(), shard);
    }

    String out = CQSMessagePopulator.getPurgeQueueResponse();
    writeResponse(out, response);

    return true;
  }
}
