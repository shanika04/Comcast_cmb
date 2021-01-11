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
package com.comcast.cns.persistence;

import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbColumn;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbColumnSlice;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbComposite;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.model.CNSSubscription;
import com.comcast.cns.model.CNSSubscription.CnsSubscriptionProtocol;
import com.comcast.cns.model.CNSSubscriptionAttributes;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.util.Util;
import com.comcast.cqs.model.CQSQueue;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;

/**
 * Column-name for CNSTopicSubscriptions table is composite(Endpoint, Protocol). row-key is topicARn
 *
 * @author aseem, bwolf, vvenkatraman, tina, jorge
 *     <p>Class is immutable
 */
public class CNSSubscriptionCassandraPersistence implements ICNSSubscriptionPersistence {

  private static Logger logger = Logger.getLogger(CNSSubscriptionCassandraPersistence.class);

  private static final String columnFamilySubscriptions = "CNSTopicSubscriptions";
  private static final String columnFamilySubscriptionsIndex = "CNSTopicSubscriptionsIndex";
  private static final String columnFamilySubscriptionsUserIndex = "CNSTopicSubscriptionsUserIndex";
  private static final String columnFamilySubscriptionsTokenIndex =
      "CNSTopicSubscriptionsTokenIndex";
  private static final String columnFamilyTopicStats = "CNSTopicStats";

  private static final AbstractDurablePersistence cassandraHandler =
      DurablePersistenceFactory.getInstance();

  public CNSSubscriptionCassandraPersistence() {}

  @SuppressWarnings("serial")
  private Map<String, String> getIndexColumnValues(
      final String endpoint, final CnsSubscriptionProtocol protocol) {
    return new HashMap<String, String>() {
      {
        put(getEndpointAndProtoIndexVal(endpoint, protocol), "");
      }
    };
  }

  private String getColumnValuesJSON(CNSSubscription s) throws JSONException {

    Writer writer = new StringWriter();
    JSONWriter jw = new JSONWriter(writer);
    jw = jw.object();

    if (s.getEndpoint() != null) {
      jw.key("endPoint").value(s.getEndpoint());
    }

    if (s.getToken() != null) {
      jw.key("token").value(s.getToken());
    }

    if (s.getArn() != null) {
      jw.key("subArn").value(s.getArn());
    }

    if (s.getUserId() != null) {
      jw.key("userId").value(s.getUserId());
    }

    if (s.getConfirmDate() != null) {
      jw.key("confirmDate").value(s.getConfirmDate().getTime() + "");
    }

    if (s.getProtocol() != null) {
      jw.key("protocol").value(s.getProtocol().toString());
    }

    if (s.getRequestDate() != null) {
      jw.key("requestDate").value(s.getRequestDate().getTime() + "");
    }

    jw.key("authenticateOnSubscribe").value(s.isAuthenticateOnUnsubscribe() + "");
    jw.key("isConfirmed").value(s.isConfirmed() + "");
    jw.key("rawMessageDelivery").value(s.getRawMessageDelivery() + "");

    jw.endObject();

    return writer.toString();
  }

  /*private Map<String, String> getColumnValues(CNSSubscription s) {

  	Map<String, String> columnValues = new HashMap<String, String>();

  	if (s.getEndpoint() != null) {
  		columnValues.put("endPoint", s.getEndpoint());
  	}

  	if (s.getToken() != null) {
  		columnValues.put("token", s.getToken());
  	}

  	if (s.getArn() != null) {
  	    columnValues.put("subArn", s.getArn());
  	}

  	if (s.getUserId() != null) {
  		columnValues.put("userId", s.getUserId());
  	}

  	if (s.getConfirmDate() != null) {
  		columnValues.put("confirmDate", s.getConfirmDate().getTime() + "");
  	}

  	if (s.getProtocol() != null) {
  		columnValues.put("protocol", s.getProtocol().toString());
  	}

  	if (s.getRequestDate() != null) {
  		columnValues.put("requestDate", s.getRequestDate().getTime() + "");
  	}

  	columnValues.put("authenticateOnSubscribe", s.isAuthenticateOnUnsubscribe() + "");
  	columnValues.put("isConfirmed", s.isConfirmed() + "");
  	columnValues.put("rawMessageDelivery", s.getRawMessageDelivery() + "");

  	return columnValues;
  }*/

  /*private static CNSSubscription getSubscriptionFromMap(Map<String, String> map) {

      String subArn = map.get("subArn");

      CNSSubscription s = new CNSSubscription(subArn);

      s.setEndpoint(map.get("endPoint"));
      s.setUserId(map.get("userId"));

      String confirmDate = map.get("confirmDate");

      if (confirmDate != null) {
      	s.setConfirmDate(new Date(Long.parseLong(confirmDate)));
      }

         String requestDate = map.get("requestDate");

         if (requestDate != null) {
         	s.setRequestDate(new Date(Long.parseLong(requestDate)));
         }

         String protocol = map.get("protocol");

         if (protocol != null) {
         	s.setProtocol(CnsSubscriptionProtocol.valueOf(protocol));
         }

         String isConfirmed = map.get("isConfirmed");

         if (isConfirmed != null) {
         	s.setConfirmed(Boolean.parseBoolean(isConfirmed));
         }

         s.setToken(map.get("token"));

         String authenticateOnSubscribe = map.get("authenticateOnSubscribe");

         if (authenticateOnSubscribe != null) {
         	s.setAuthenticateOnUnsubscribe(Boolean.parseBoolean(authenticateOnSubscribe));
         }

         String rawMessage = map.get("rawMessageDelivery");
         if (rawMessage != null) {
         	s.setRawMessageDelivery(Boolean.parseBoolean(rawMessage));
         }

         return s;
  }*/

  private static String getEndpointAndProtoIndexVal(
      String endpoint, CnsSubscriptionProtocol protocol) {
    return protocol.name() + ":" + endpoint;
  }

  private static String getEndpointAndProtoIndexValEndpoint(String composite) {
    String[] arr = composite.split(":");
    if (arr.length < 2) {
      throw new IllegalArgumentException(
          "Bad format for EndpointAndProtocol composite. Must be of the form <protocol>:<endpoint>. Got:"
              + composite);
    }
    StringBuffer sb = new StringBuffer(arr[1]);
    for (int i = 2; i < arr.length; i++) {
      sb.append(":").append(arr[i]);
    }
    return sb.toString();
  }

  private static CnsSubscriptionProtocol getEndpointAndProtoIndexValProtocol(String composite) {
    String[] arr = composite.split(":");
    if (arr.length < 2) {
      throw new IllegalArgumentException(
          "Bad format for EndpointAndProtocol composite. Must be of the form <protocol>:<endpoint>. Got:"
              + composite);
    }
    return CnsSubscriptionProtocol.valueOf(arr[0]);
  }

  private void insertOrUpdateSubsAndIndexes(final CNSSubscription subscription, Integer ttl)
      throws Exception {
    subscription.checkIsValid();
    CmbComposite columnName =
        cassandraHandler.getCmbComposite(
            subscription.getEndpoint(), subscription.getProtocol().name());
    cassandraHandler.update(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptions,
        subscription.getTopicArn(),
        columnName,
        getColumnValuesJSON(subscription),
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.COMPOSITE_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        ttl);
    cassandraHandler.insertRow(
        AbstractDurablePersistence.CNS_KEYSPACE,
        subscription.getArn(),
        columnFamilySubscriptionsIndex,
        getIndexColumnValues(subscription.getEndpoint(), subscription.getProtocol()),
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        ttl);
    cassandraHandler.insertRow(
        AbstractDurablePersistence.CNS_KEYSPACE,
        subscription.getUserId(),
        columnFamilySubscriptionsUserIndex,
        new HashMap<String, String>() {
          {
            put(subscription.getArn(), "");
          }
        },
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        ttl);
    cassandraHandler.insertRow(
        AbstractDurablePersistence.CNS_KEYSPACE,
        subscription.getToken(),
        columnFamilySubscriptionsTokenIndex,
        new HashMap<String, String>() {
          {
            put(subscription.getArn(), "");
          }
        },
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER,
        ttl);
  }

  @Override
  public CNSSubscription subscribe(
      String endpoint, CnsSubscriptionProtocol protocol, String topicArn, String userId)
      throws Exception {

    // subscription is unique by protocol + endpoint + topic

    final CNSSubscription subscription = new CNSSubscription(endpoint, protocol, topicArn, userId);

    CNSTopic t = PersistenceFactory.getTopicPersistence().getTopic(topicArn);

    if (t == null) {
      throw new TopicNotFoundException("Resource not found.");
    }

    // check if queue exists for cqs endpoints

    if (protocol.equals(CnsSubscriptionProtocol.cqs)) {

      CQSQueue queue =
          PersistenceFactory.getQueuePersistence()
              .getQueue(com.comcast.cqs.util.Util.getRelativeQueueUrlForArn(endpoint));

      if (queue == null) {
        throw new CMBException(
            CMBErrorCodes.NotFound, "Queue with arn " + endpoint + " does not exist.");
      }
    }

    subscription.setArn(Util.generateCnsTopicSubscriptionArn(topicArn, protocol, endpoint));

    // attempt to delete existing subscription

    /*Composite superColumnName = new Composite(subscription.getEndpoint(), subscription.getProtocol().name());

    HSuperColumn<Composite, String, String> superCol = readColumnFromSuperColumnFamily(columnFamilySubscriptions, subscription.getTopicArn(), superColumnName, new StringSerializer(), new CompositeSerializer(), StringSerializer.get(), StringSerializer.get(), CMBProperties.getInstance().getReadConsistencyLevel());

    if (superCol != null) {
    	CNSSubscription exisitingSub = extractSubscriptionFromSuperColumn(superCol, topicArn);
              deleteIndexes(exisitingSub.getArn(), exisitingSub.getUserId(), exisitingSub.getToken());
    	deleteSuperColumn(subscriptionsTemplate, exisitingSub.getTopicArn(), superColumnName);
    }*/

    // then set confirmation stuff and update cassandra

    CNSSubscription retrievedSubscription = getSubscription(subscription.getArn());

    if (!CMBProperties.getInstance().getCNSRequireSubscriptionConfirmation()) {

      subscription.setConfirmed(true);
      subscription.setConfirmDate(new Date());

      insertOrUpdateSubsAndIndexes(subscription, null);

      if (retrievedSubscription == null) {
        cassandraHandler.incrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            subscription.getTopicArn(),
            "subscriptionConfirmed",
            1,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      }

    } else {

      // protocols that cannot confirm subscriptions (e.g. redisPubSub)
      // get an automatic confirmation here
      if (!protocol.canConfirmSubscription()) {
        subscription.setConfirmed(true);
        subscription.setConfirmDate(new Date());
        insertOrUpdateSubsAndIndexes(subscription, null);

        // auto confirm subscription to cqs queue by owner
      } else if (protocol.equals(CnsSubscriptionProtocol.cqs)) {

        String queueOwner = com.comcast.cqs.util.Util.getQueueOwnerFromArn(endpoint);

        if (queueOwner != null && queueOwner.equals(userId)) {

          subscription.setConfirmed(true);
          subscription.setConfirmDate(new Date());

          insertOrUpdateSubsAndIndexes(subscription, null);
          if (retrievedSubscription == null) {
            cassandraHandler.incrementCounter(
                AbstractDurablePersistence.CNS_KEYSPACE,
                columnFamilyTopicStats,
                subscription.getTopicArn(),
                "subscriptionConfirmed",
                1,
                CMB_SERIALIZER.STRING_SERIALIZER,
                CMB_SERIALIZER.STRING_SERIALIZER);
          }
        } else {

          // use cassandra ttl to implement expiration after 3 days
          insertOrUpdateSubsAndIndexes(subscription, 3 * 24 * 60 * 60);
          if (retrievedSubscription == null) {
            cassandraHandler.incrementCounter(
                AbstractDurablePersistence.CNS_KEYSPACE,
                columnFamilyTopicStats,
                subscription.getTopicArn(),
                "subscriptionPending",
                1,
                CMB_SERIALIZER.STRING_SERIALIZER,
                CMB_SERIALIZER.STRING_SERIALIZER);
          }
        }

      } else {

        // use cassandra ttl to implement expiration after 3 days
        insertOrUpdateSubsAndIndexes(subscription, 3 * 24 * 60 * 60);
        if (retrievedSubscription == null) {
          cassandraHandler.incrementCounter(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilyTopicStats,
              subscription.getTopicArn(),
              "subscriptionPending",
              1,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);
        }
      }
    }

    CNSSubscriptionAttributes attributes =
        new CNSSubscriptionAttributes(topicArn, subscription.getArn(), userId);
    PersistenceFactory.getCNSAttributePersistence()
        .setSubscriptionAttributes(attributes, subscription.getArn());

    return subscription;
  }

  @Override
  public CNSSubscription getSubscription(String arn) throws Exception {

    // read form index to get composite col-name

    CmbColumnSlice<String, String> slice =
        cassandraHandler.readColumnSlice(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilySubscriptionsIndex,
            arn,
            null,
            null,
            1,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (slice != null) {

      // get Column from main table

      String colName = slice.getColumns().get(0).getName();
      CnsSubscriptionProtocol protocol = getEndpointAndProtoIndexValProtocol(colName);
      String endpoint = getEndpointAndProtoIndexValEndpoint(colName);
      CmbComposite columnName = cassandraHandler.getCmbComposite(endpoint, protocol.name());
      CmbColumn<CmbComposite, String> column =
          cassandraHandler.readColumn(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilySubscriptions,
              Util.getCnsTopicArn(arn),
              columnName,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.COMPOSITE_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (column != null) {
        CNSSubscription s = extractSubscriptionFromColumn(column, Util.getCnsTopicArn(arn));
        s.checkIsValid();
        return s;
      }
    }

    return null;
  }

  private static CNSSubscription extractSubscriptionFromColumn(
      CmbColumn<CmbComposite, String> column, String topicArn) throws JSONException {

    JSONObject json = new JSONObject(column.getValue());
    CNSSubscription s = new CNSSubscription(json.getString("subArn"));

    s.setEndpoint(json.getString("endPoint"));
    s.setUserId(json.getString("userId"));

    if (json.has("confirmDate")) {
      s.setConfirmDate(new Date(json.getLong("confirmDate")));
    }

    if (json.has("requestDate")) {
      s.setRequestDate(new Date(json.getLong("requestDate")));
    }

    if (json.has("protocol")) {
      s.setProtocol(CnsSubscriptionProtocol.valueOf(json.getString("protocol")));
    }

    if (json.has("isConfirmed")) {
      s.setConfirmed(json.getBoolean("isConfirmed"));
    }

    s.setToken(json.getString("token"));

    if (json.has("authenticateOnSubscribe")) {
      s.setAuthenticateOnUnsubscribe(json.getBoolean("authenticateOnSubscribe"));
    }

    if (json.has("rawMessageDelivery")) {
      s.setRawMessageDelivery(json.getBoolean("rawMessageDelivery"));
    }

    s.setTopicArn(topicArn);

    return s;
  }

  /*private static CNSSubscription extractSubscriptionFromSuperColumn(CmbSuperColumn<CmbComposite, String, String> superCol, String topicArn) {

      Map<String, String> messageMap = new HashMap<String, String>(superCol.getColumns().size());

         for (CmbColumn<String, String> column : superCol.getColumns()) {
             messageMap.put(column.getName(), column.getValue());
         }

         CNSSubscription sub = getSubscriptionFromMap(messageMap);
         sub.setTopicArn(topicArn);

         return sub;
  }*/

  @Override
  /**
   * List all subscription for a user, unconfirmed subscriptions will not reveal their arns.
   * Pagination for more than 100 subscriptions.
   *
   * @param nextToken initially null, on subsequent calls arn of last result from prior call
   * @param protocol optional filter by protocol (this parameter is not part of official AWS API)
   * @return list of subscriptions. If nextToken is not null, the subscription corresponding to it
   *     is not returned.
   * @throws Exception
   */
  public List<CNSSubscription> listSubscriptions(
      String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception {
    return listSubscriptions(nextToken, protocol, userId, true);
  }

  private List<CNSSubscription> listSubscriptions(
      String nextToken, CnsSubscriptionProtocol protocol, String userId, boolean hidePendingArn)
      throws Exception {

    if (nextToken != null) {
      if (getSubscription(nextToken) == null) {
        throw new SubscriberNotFoundException("Subscriber not found for arn " + nextToken);
      }
    }

    // Algorithm is to keep reading in chunks of 100 till we've seen 500 from the nextToken-ARN
    List<CNSSubscription> l = new ArrayList<CNSSubscription>();
    // read form index to get sub-arn

    while (l.size() < 100) {

      CmbColumnSlice<String, String> slice =
          cassandraHandler.readColumnSlice(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilySubscriptionsUserIndex,
              userId,
              nextToken,
              null,
              500,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (slice == null) {
        return l;
      }

      // get Column from main table
      List<CmbColumn<String, String>> cols = slice.getColumns();

      if (nextToken != null) {
        cols.remove(0);
      }

      if (cols.size() == 0) {
        return l;
      }

      for (CmbColumn<String, String> col : cols) {

        String subArn = col.getName();
        CNSSubscription subscription = getSubscription(subArn);

        if (subscription == null) {
          throw new IllegalStateException(
              "Subscriptions-user-index contains subscription-arn which doesn't exist in subscriptions-index. subArn:"
                  + subArn);
        }

        // ignore invalid subscriptions coming from Cassandra

        try {
          subscription.checkIsValid();
        } catch (CMBException ex) {
          logger.error("event=invalid_subscription " + subscription.toString(), ex);
          continue;
        }

        if (protocol != null && subscription.getProtocol() != protocol) {
          continue;
        }

        if (hidePendingArn) {
          if (subscription.isConfirmed()) {
            l.add(subscription);
          } else {
            subscription.setArn("PendingConfirmation");
            l.add(subscription);
          }
        } else {
          l.add(subscription);
        }

        if (l.size() == 100) {
          return l;
        }
      }

      nextToken = cols.get(cols.size() - 1).getName();
    }

    return l;
  }

  @Override
  public List<CNSSubscription> listAllSubscriptions(
      String nextToken, CnsSubscriptionProtocol protocol, String userId) throws Exception {
    return listSubscriptions(nextToken, protocol, userId, false);
  }

  @Override
  public List<CNSSubscription> listSubscriptionsByTopic(
      String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception {
    return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100);
  }

  @Override
  public List<CNSSubscription> listSubscriptionsByTopic(
      String nextToken, String topicArn, CnsSubscriptionProtocol protocol, int pageSize)
      throws Exception {
    return listSubscriptionsByTopic(nextToken, topicArn, protocol, pageSize, true);
  }
  /**
   * Enumerate all subs in a topic
   *
   * @param nextToken The ARN of the last sub-returned or null is first time call.
   * @param topicArn
   * @param protocol
   * @param pageSize
   * @param hidePendingArn
   * @return The list of subscriptions given a topic. Note: if nextToken is provided, the returned
   *     list will not contain it for convenience
   * @throws Exception
   */
  public List<CNSSubscription> listSubscriptionsByTopic(
      String nextToken,
      String topicArn,
      CnsSubscriptionProtocol protocol,
      int pageSize,
      boolean hidePendingArn)
      throws Exception {

    if (nextToken != null) {
      if (getSubscription(nextToken) == null) {
        throw new SubscriberNotFoundException("Subscriber not found for arn " + nextToken);
      }
    }

    // read from index to get composite-col-name corresponding to nextToken
    CmbComposite nextTokenComposite = null;

    if (nextToken != null) {
      CmbColumnSlice<String, String> slice =
          cassandraHandler.readColumnSlice(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilySubscriptionsIndex,
              nextToken,
              null,
              null,
              1,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);
      if (slice == null) {
        throw new IllegalArgumentException("Could not find any subscription with arn " + nextToken);
      }
      // get Column from main table
      String colName = slice.getColumns().get(0).getName();
      CnsSubscriptionProtocol tokProtocol = getEndpointAndProtoIndexValProtocol(colName);
      String endpoint = getEndpointAndProtoIndexValEndpoint(colName);
      nextTokenComposite = cassandraHandler.getCmbComposite(endpoint, tokProtocol.name());
    }

    List<CNSSubscription> l = new ArrayList<CNSSubscription>();

    CNSTopic t = PersistenceFactory.getTopicPersistence().getTopic(topicArn);

    if (t == null) {
      throw new TopicNotFoundException("Resource not found.");
    }

    // read pageSize at a time

    CmbColumnSlice<CmbComposite, String> cols =
        cassandraHandler.readColumnSlice(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilySubscriptions,
            topicArn,
            nextTokenComposite,
            null,
            pageSize,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.COMPOSITE_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (nextToken != null && cols.size() > 0) {
      cols.getColumns().remove(0);
    }

    while (l.size() < pageSize) {

      if (cols == null || cols.size() == 0) {
        return l;
      }

      for (CmbColumn<CmbComposite, String> col : cols.getColumns()) {

        CNSSubscription sub = extractSubscriptionFromColumn(col, topicArn);

        // ignore invalid subscriptions coming from Cassandra

        try {
          sub.checkIsValid();
        } catch (CMBException ex) {
          logger.error("event=invalid_subscription " + sub.toString(), ex);
          continue;
        }

        if (protocol != null && protocol != sub.getProtocol()) {
          continue;
        }

        if (hidePendingArn) {
          if (sub.isConfirmed()) {
            l.add(sub);
          } else {
            sub.setArn("PendingConfirmation");
            l.add(sub);
          }
        } else {
          l.add(sub);
        }

        if (l.size() == pageSize) {
          return l;
        }
      }

      nextTokenComposite = cols.getColumns().get(cols.size() - 1).getName();
      cols =
          cassandraHandler.readColumnSlice(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilySubscriptions,
              topicArn,
              nextTokenComposite,
              null,
              pageSize,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.COMPOSITE_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (cols.size() > 0) {
        cols.getColumns().remove(0);
      }
    }

    return l;
  }

  @Override
  public List<CNSSubscription> listAllSubscriptionsByTopic(
      String nextToken, String topicArn, CnsSubscriptionProtocol protocol) throws Exception {
    return listSubscriptionsByTopic(nextToken, topicArn, protocol, 100, false);
  }

  @Override
  public CNSSubscription confirmSubscription(
      boolean authenticateOnUnsubscribe, String token, String topicArn) throws Exception {

    // get Sub-arn given token
    CmbColumnSlice<String, String> slice =
        cassandraHandler.readColumnSlice(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilySubscriptionsTokenIndex,
            token,
            null,
            null,
            1,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (slice == null) {
      throw new CMBException(CMBErrorCodes.NotFound, "Resource not found.");
    }

    // get Column from main table
    String subArn = slice.getColumns().get(0).getName();

    // get Subscription given subArn
    final CNSSubscription s = getSubscription(subArn);

    if (s == null) {
      throw new SubscriberNotFoundException(
          "Could not find subscription given subscription arn " + subArn);
    }

    s.setAuthenticateOnUnsubscribe(authenticateOnUnsubscribe);
    s.setConfirmed(true);
    s.setConfirmDate(new Date());

    // re-insert with no TTL. will clobber the old one which had ttl
    insertOrUpdateSubsAndIndexes(s, null);

    cassandraHandler.decrementCounter(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilyTopicStats,
        s.getTopicArn(),
        "subscriptionPending",
        1,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.incrementCounter(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilyTopicStats,
        s.getTopicArn(),
        "subscriptionConfirmed",
        1,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);

    return s;
  }

  private void deleteIndexesAll(List<CNSSubscription> subscriptionList)
      throws PersistenceException {
    List<String> subArnList = new LinkedList<String>();
    List<String> userIdList = new LinkedList<String>();
    List<String> tokenList = new LinkedList<String>();
    for (CNSSubscription sub : subscriptionList) {
      subArnList.add(sub.getArn());
      userIdList.add(sub.getUserId());
      tokenList.add(sub.getToken());
    }
    cassandraHandler.deleteBatch(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptionsIndex,
        subArnList,
        null,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.deleteBatch(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptionsUserIndex,
        userIdList,
        subArnList,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.deleteBatch(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptionsTokenIndex,
        tokenList,
        subArnList,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
  }

  private void deleteIndexes(String subArn, String userId, String token)
      throws PersistenceException {
    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptionsIndex,
        subArn,
        null,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptionsUserIndex,
        userId,
        subArn,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptionsTokenIndex,
        token,
        subArn,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
  }

  @Override
  public void unsubscribe(String arn) throws Exception {

    CNSSubscription s = getSubscription(arn);

    if (s != null) {

      deleteIndexes(arn, s.getUserId(), s.getToken());
      CmbComposite columnName =
          cassandraHandler.getCmbComposite(s.getEndpoint(), s.getProtocol().name());
      cassandraHandler.delete(
          AbstractDurablePersistence.CNS_KEYSPACE,
          columnFamilySubscriptions,
          Util.getCnsTopicArn(arn),
          columnName,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.COMPOSITE_SERIALIZER);

      if (s.isConfirmed()) {
        cassandraHandler.decrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            s.getTopicArn(),
            "subscriptionConfirmed",
            1,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      } else {
        cassandraHandler.decrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            s.getTopicArn(),
            "subscriptionPending",
            1,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      }

      cassandraHandler.incrementCounter(
          AbstractDurablePersistence.CNS_KEYSPACE,
          columnFamilyTopicStats,
          s.getTopicArn(),
          "subscriptionDeleted",
          1,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER);
    }
  }

  public long getCountSubscription(String topicArn, String columnName) throws Exception {
    return cassandraHandler.getCounter(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilyTopicStats,
        topicArn,
        columnName,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
  }

  @Override
  public void unsubscribeAll(String topicArn) throws Exception {

    int pageSize = 1000;

    String nextToken = null;
    List<CNSSubscription> subs =
        listSubscriptionsByTopic(nextToken, topicArn, null, pageSize, false);

    // for pagination to work we need the nextToken's corresponding sub to not be deleted.

    CNSSubscription nextTokenSub = null;

    while (subs.size() > 0) {

      if (subs.size() < pageSize) {
        deleteIndexesAll(subs);
        cassandraHandler.incrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            topicArn,
            "subscriptionDeleted",
            subs.size(),
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
        break;
      } else {
        // keep the last subscription for pagination purpose.
        nextTokenSub = subs.get(subs.size() - 1);
        nextToken = nextTokenSub.getArn();
        subs.remove(subs.size() - 1);
        deleteIndexesAll(subs);
        subs = listSubscriptionsByTopic(nextToken, topicArn, null, pageSize, false);
        deleteIndexes(nextTokenSub.getArn(), nextTokenSub.getUserId(), nextTokenSub.getToken());
        cassandraHandler.incrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            topicArn,
            "subscriptionDeleted",
            subs.size(),
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      }
    }

    long subscriptionConfirmed =
        cassandraHandler.getCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            topicArn,
            "subscriptionConfirmed",
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (subscriptionConfirmed > 0) {
      cassandraHandler.decrementCounter(
          AbstractDurablePersistence.CNS_KEYSPACE,
          columnFamilyTopicStats,
          topicArn,
          "subscriptionConfirmed",
          (int) subscriptionConfirmed,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER);
    }

    long subscriptionPending =
        cassandraHandler.getCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            topicArn,
            "subscriptionPending",
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (subscriptionPending > 0) {
      cassandraHandler.decrementCounter(
          AbstractDurablePersistence.CNS_KEYSPACE,
          columnFamilyTopicStats,
          topicArn,
          "subscriptionPending",
          (int) subscriptionPending,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER);
    }

    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilySubscriptions,
        topicArn,
        null,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
  }

  @Override
  public void setRawMessageDelivery(String subscriptionArn, boolean rawMessageDelivery)
      throws Exception {
    CNSSubscription sub;
    sub = getSubscription(subscriptionArn);
    if (sub != null) {
      sub.setRawMessageDelivery(rawMessageDelivery);
      insertOrUpdateSubsAndIndexes(sub, null);
    }
  }
}
