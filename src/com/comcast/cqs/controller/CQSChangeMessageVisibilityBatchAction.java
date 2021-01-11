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
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSBatchResultErrorEntry;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import com.comcast.cqs.util.CQSErrorCodes;
import com.comcast.cqs.util.Util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Change message visibility batch action
 *
 * @author baosen, vvenkatraman, bwolf
 */
public class CQSChangeMessageVisibilityBatchAction extends CQSAction {

  public CQSChangeMessageVisibilityBatchAction() {
    super("ChangeMessageVisibilityBatch");
  }

  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    CQSQueue queue = CQSCache.getCachedQueue(user, request);
    List<String> idList = new ArrayList<String>();
    List<CQSBatchResultErrorEntry> failedList = new ArrayList<CQSBatchResultErrorEntry>();
    Map<String, List<String>> idMap = new HashMap<String, List<String>>();
    int index = 1;

    String suppliedId =
        request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + ".Id");
    String receiptHandle =
        request.getParameter(
            this.actionName
                + CQSConstants.REQUEST_ENTRY
                + index
                + "."
                + CQSConstants.RECEIPT_HANDLE);

    while (suppliedId != null && receiptHandle != null) {

      if (!Util.isValidId(suppliedId)) {
        throw new CMBException(
            CQSErrorCodes.InvalidBatchEntryId,
            "Id "
                + suppliedId
                + " is invalid. Only alphanumeric, hyphen, and underscore are allowed. It can be at most "
                + CMBProperties.getInstance().getCQSMaxMessageSuppliedIdLength()
                + " letters long.");
      }

      if (idList.contains(suppliedId)) {
        throw new CMBException(
            CQSErrorCodes.BatchEntryIdsNotDistinct, "Id " + suppliedId + " repeated");
      }

      idList.add(suppliedId);

      if (receiptHandle.isEmpty()) {
        failedList.add(
            new CQSBatchResultErrorEntry(
                suppliedId,
                true,
                "ReceiptHandleIsInvalid",
                "No Value Found for "
                    + this.actionName
                    + CQSConstants.REQUEST_ENTRY
                    + index
                    + "."
                    + CQSConstants.RECEIPT_HANDLE));
      } else {

        String visibilityTimeoutStr =
            request.getParameter(
                this.actionName
                    + CQSConstants.REQUEST_ENTRY
                    + index
                    + "."
                    + CQSConstants.VISIBILITY_TIMEOUT);

        if (visibilityTimeoutStr != null) {

          Integer visibilityTimeout = Integer.parseInt(visibilityTimeoutStr);

          if (visibilityTimeout < 0
              || visibilityTimeout > CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()) {
            throw new CMBException(
                CMBErrorCodes.InvalidParameterValue,
                "VisibilityTimeout is limited from 0 to "
                    + CMBProperties.getInstance().getCQSMaxVisibilityTimeOut()
                    + " seconds");
          }
        }
        idMap.put(suppliedId, Arrays.asList(receiptHandle, visibilityTimeoutStr));
      }

      index++;
      suppliedId =
          request.getParameter(this.actionName + CQSConstants.REQUEST_ENTRY + index + ".Id");
      receiptHandle =
          request.getParameter(
              this.actionName
                  + CQSConstants.REQUEST_ENTRY
                  + index
                  + "."
                  + CQSConstants.RECEIPT_HANDLE);
    }

    if (idMap.size() == 0) {
      throw new CMBException(
          CMBErrorCodes.InvalidQueryParameter,
          "Both user supplied message Id and receiptHandle are required");
    }

    List<String> successList = new ArrayList<String>();

    for (Map.Entry<String, List<String>> entry : idMap.entrySet()) {

      if (PersistenceFactory.getCQSMessagePersistence()
          .changeMessageVisibility(
              queue, entry.getValue().get(0), Integer.parseInt(entry.getValue().get(1)))) {
        successList.add(entry.getKey());
      } else {
        failedList.add(
            new CQSBatchResultErrorEntry(
                entry.getKey(),
                true,
                "ReceiptHandleIsInvalid",
                "The input receipt handle is invalid."));
      }
    }

    String out =
        CQSMessagePopulator.getChangeMessageVisibilityBatchResponse(successList, failedList);
    writeResponse(out, response);

    return true;
  }
}
