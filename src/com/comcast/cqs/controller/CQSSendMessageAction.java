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
import com.comcast.cmb.common.util.Util;
import com.comcast.cqs.io.CQSMessagePopulator;
import com.comcast.cqs.model.CQSMessage;
import com.comcast.cqs.model.CQSMessageAttribute;
import com.comcast.cqs.model.CQSQueue;
import com.comcast.cqs.util.CQSConstants;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * Send message
 *
 * @author aseem, bwolf, vvenkatraman, baosen
 */
public class CQSSendMessageAction extends CQSAction {

  protected static Logger logger = Logger.getLogger(CQSSendMessageAction.class);
  private static Random rand = new Random();

  public CQSSendMessageAction() {
    super("SendMessage");
  }

  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    CQSHttpServletRequest request = (CQSHttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    String messageBody = request.getParameter(CQSConstants.MESSAGE_BODY);

    if (messageBody == null) {
      throw new CMBException(CMBErrorCodes.MissingParameter, "MessageBody not found");
    }

    if (!Util.isValidUnicode(messageBody)) {
      throw new CMBException(
          CMBErrorCodes.InvalidMessageContents,
          "The message contains characters outside the allowed set.");
    }

    HashMap<String, String> attributes = new HashMap<String, String>();

    String delaySecondsReq = request.getParameter(CQSConstants.DELAY_SECONDS);

    if (delaySecondsReq != null) {

      try {

        int delaySeconds = Integer.parseInt(delaySecondsReq);

        if (delaySeconds < 0
            || delaySeconds > CMBProperties.getInstance().getCQSMaxMessageDelaySeconds()) {
          throw new CMBException(
              CMBErrorCodes.InvalidParameterValue,
              "DelaySeconds should be from 0 to "
                  + CMBProperties.getInstance().getCQSMaxMessageDelaySeconds());
        } else {
          attributes.put(CQSConstants.DELAY_SECONDS, "" + delaySeconds);
        }

      } catch (NumberFormatException e) {
        throw new CMBException(
            CMBErrorCodes.InvalidParameterValue, "DelaySeconds must be integer value");
      }
    }

    attributes.put(CQSConstants.SENDER_ID, user.getUserId());
    attributes.put(CQSConstants.SENT_TIMESTAMP, "" + System.currentTimeMillis());
    attributes.put(CQSConstants.APPROXIMATE_RECEIVE_COUNT, "0");
    attributes.put(CQSConstants.APPROXIMATE_FIRST_RECEIVE_TIMESTAMP, "");

    Map<String, CQSMessageAttribute> messageAttributes = new HashMap<String, CQSMessageAttribute>();

    int index = 1;
    String messageAttributeName =
        request.getParameter(CQSConstants.MESSAGE_ATTRIBUTE + "." + index + ".Name");
    int messageAttributeSize = 0;

    while (messageAttributeName != null && !messageAttributeName.equals("")) {
      String messageAttributeValue =
          request.getParameter(CQSConstants.MESSAGE_ATTRIBUTE + "." + index + ".Value.StringValue");
      if (messageAttributeValue == null || messageAttributeValue.equals("")) {
        messageAttributeValue =
            request.getParameter(
                CQSConstants.MESSAGE_ATTRIBUTE + "." + index + ".Value.BinaryValue");
      }
      if (messageAttributeValue == null || messageAttributeValue.equals("")) {
        throw new CMBException(
            CMBErrorCodes.InvalidParameterValue,
            "Missing message attribute value " + messageAttributeName);
      }
      String messageAttributeDataType =
          request.getParameter(CQSConstants.MESSAGE_ATTRIBUTE + "." + index + ".Value.DataType");
      if (messageAttributeDataType == null || messageAttributeDataType.equals("")) {
        throw new CMBException(
            CMBErrorCodes.InvalidParameterValue,
            "Missing message attribute data type " + messageAttributeName);
      }
      messageAttributeSize += messageAttributeValue.length();
      messageAttributes.put(
          messageAttributeName,
          new CQSMessageAttribute(messageAttributeValue, messageAttributeDataType));
      index++;
      messageAttributeName =
          request.getParameter(CQSConstants.MESSAGE_ATTRIBUTE + "." + index + ".Name");
    }

    if (messageBody.length() + messageAttributeSize
        > CMBProperties.getInstance().getCQSMaxMessageSize()) {
      throw new CMBException(
          CMBErrorCodes.InvalidParameterValue,
          "Value for parameter MessageBody and/or message attributes are invalid. Reason: Total size must be shorter than "
              + CMBProperties.getInstance().getCQSMaxMessageSize()
              + " bytes");
    }

    CQSMessage message = new CQSMessage(messageBody, attributes, messageAttributes);
    CQSQueue queue = CQSCache.getCachedQueue(user, request);

    int shard = 0;

    String shardNumber = request.getParameter("Shard");

    if (shardNumber != null) {
      shard = Integer.parseInt(shardNumber);
    } else if (queue.getNumberOfShards() > 1) {
      shard = rand.nextInt(queue.getNumberOfShards());
    }

    if (shard < 0 || shard >= queue.getNumberOfShards()) {
      throw new CMBException(
          CMBErrorCodes.InvalidParameterValue,
          "Shard number "
              + shard
              + " exceeds number of available shards in queue ("
              + queue.getNumberOfShards()
              + ").");
    }

    String receiptHandle =
        PersistenceFactory.getCQSMessagePersistence().sendMessage(queue, shard, message);

    if (receiptHandle == null || receiptHandle.isEmpty()) {
      throw new CMBException(CMBErrorCodes.InternalError, "Failed to add message to queue");
    }

    request.setReceiptHandles(Arrays.asList(new String[] {receiptHandle}));
    message.setReceiptHandle(receiptHandle);
    message.setMessageId(receiptHandle);
    String out = CQSMessagePopulator.getSendMessageResponse(message);
    writeResponse(out, response);
    CQSMonitor.getInstance().addNumberOfMessagesReceived(queue.getRelativeUrl(), 1);

    try {
      CQSLongPollSender.send(queue.getArn());
    } catch (Exception ex) {
      logger.warn("event=failed_to_send_longpoll_notification", ex);
    }

    return true;
  }
}
