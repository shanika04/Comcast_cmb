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
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cns.controller.CNSCache;
import com.comcast.cns.model.CNSTopic;
import com.comcast.cns.model.CNSTopicAttributes;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.Util;
import com.comcast.cqs.util.CQSErrorCodes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Provide Cassandra persistence for topics
 *
 * @author aseem, bwolf, jorge, vvenkatraman, tina, michael
 *     <p>Class is immutable
 */
public class CNSTopicCassandraPersistence implements ICNSTopicPersistence {

  private static Logger logger = Logger.getLogger(CNSTopicCassandraPersistence.class);

  private static final String columnFamilyTopics = "CNSTopics";
  private static final String columnFamilyTopicsByUserId = "CNSTopicsByUserId";
  private static final String columnFamilyTopicAttributes = "CNSTopicAttributes";
  private static final String columnFamilyTopicStats = "CNSTopicStats";

  private static final AbstractDurablePersistence cassandraHandler =
      DurablePersistenceFactory.getInstance();

  public CNSTopicCassandraPersistence() {}

  private Map<String, String> getColumnValues(CNSTopic t) {

    Map<String, String> columnValues = new HashMap<String, String>();

    if (t.getUserId() != null) {
      columnValues.put("userId", t.getUserId());
    }

    if (t.getName() != null) {
      columnValues.put("name", t.getName());
    }

    if (t.getDisplayName() != null) {
      columnValues.put("displayName", t.getDisplayName());
    }

    return columnValues;
  }

  @Override
  public CNSTopic createTopic(String name, String displayName, String userId) throws Exception {

    String arn = Util.generateCnsTopicArn(name, CMBProperties.getInstance().getRegion(), userId);

    // disable user topic limit for now

    /*List<CNSTopic> topics = listTopics(userId, null);

    if (topics.size() >= Util.CNS_USER_TOPIC_LIMIT) {
    	throw new CMBException(CNSErrorCodes.CNS_TopicLimitExceeded, "Topic limit exceeded.");
    }*/

    CNSTopic topic = getTopic(arn);

    if (topic != null) {
      return topic;
    } else {

      topic = new CNSTopic(arn, name, displayName, userId);
      topic.checkIsValid();

      cassandraHandler.insertRow(
          AbstractDurablePersistence.CNS_KEYSPACE,
          topic.getArn(),
          columnFamilyTopics,
          getColumnValues(topic),
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          null);
      cassandraHandler.update(
          AbstractDurablePersistence.CNS_KEYSPACE,
          columnFamilyTopicsByUserId,
          userId,
          topic.getArn(),
          "",
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          null);

      // note: deleteing rows or columns makes them permanently unavailable as counters!
      // http://stackoverflow.com/questions/13653681/apache-cassandra-delete-from-counter

      // cassandraHandler.delete(AbstractDurablePersistence.CNS_KEYSPACE, columnFamilyTopicStats,
      // arn, null, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);

      // cassandraHandler.deleteCounter(AbstractDurablePersistence.CNS_KEYSPACE,
      // columnFamilyTopicStats, arn, "subscriptionConfirmed", CMB_SERIALIZER.STRING_SERIALIZER,
      // CMB_SERIALIZER.STRING_SERIALIZER);
      // cassandraHandler.deleteCounter(AbstractDurablePersistence.CNS_KEYSPACE,
      // columnFamilyTopicStats, arn, "subscriptionPending", CMB_SERIALIZER.STRING_SERIALIZER,
      // CMB_SERIALIZER.STRING_SERIALIZER);
      // cassandraHandler.deleteCounter(AbstractDurablePersistence.CNS_KEYSPACE,
      // columnFamilyTopicStats, arn, "subscriptionDeleted", CMB_SERIALIZER.STRING_SERIALIZER,
      // CMB_SERIALIZER.STRING_SERIALIZER);

      long subscriptionConfirmed =
          cassandraHandler.getCounter(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilyTopicStats,
              arn,
              "subscriptionConfirmed",
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (subscriptionConfirmed > 0) {
        cassandraHandler.decrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            arn,
            "subscriptionConfirmed",
            (int) subscriptionConfirmed,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      }

      long subscriptionPending =
          cassandraHandler.getCounter(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilyTopicStats,
              arn,
              "subscriptionPending",
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (subscriptionPending > 0) {
        cassandraHandler.decrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            arn,
            "subscriptionPending",
            (int) subscriptionPending,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      }

      long subscriptionDeleted =
          cassandraHandler.getCounter(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilyTopicStats,
              arn,
              "subscriptionDeleted",
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (subscriptionDeleted > 0) {
        cassandraHandler.decrementCounter(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicStats,
            arn,
            "subscriptionDeleted",
            (int) subscriptionDeleted,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
      }

      // cassandraHandler.incrementCounter(AbstractDurablePersistence.CNS_KEYSPACE,
      // columnFamilyTopicStats, arn, "subscriptionConfirmed", 0, CMB_SERIALIZER.STRING_SERIALIZER,
      // CMB_SERIALIZER.STRING_SERIALIZER);
      // cassandraHandler.incrementCounter(AbstractDurablePersistence.CNS_KEYSPACE,
      // columnFamilyTopicStats, arn, "subscriptionPending", 0, CMB_SERIALIZER.STRING_SERIALIZER,
      // CMB_SERIALIZER.STRING_SERIALIZER);
      // cassandraHandler.incrementCounter(AbstractDurablePersistence.CNS_KEYSPACE,
      // columnFamilyTopicStats, arn, "subscriptionDeleted", 0, CMB_SERIALIZER.STRING_SERIALIZER,
      // CMB_SERIALIZER.STRING_SERIALIZER);

      CNSTopicAttributes attributes = new CNSTopicAttributes(arn, userId);
      PersistenceFactory.getCNSAttributePersistence().setTopicAttributes(attributes, arn);

      return topic;
    }
  }

  @Override
  public void deleteTopic(String arn) throws Exception {

    CNSTopic topic = getTopic(arn);

    if (topic == null) {
      throw new CMBException(CNSErrorCodes.CNS_NotFound, "Topic not found.");
    }

    // delete all subscriptions first

    PersistenceFactory.getSubscriptionPersistence().unsubscribeAll(topic.getArn());

    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilyTopics,
        arn,
        null,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilyTopicsByUserId,
        topic.getUserId(),
        arn,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
    cassandraHandler.delete(
        AbstractDurablePersistence.CNS_KEYSPACE,
        columnFamilyTopicAttributes,
        arn,
        null,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);

    // cassandraHandler.delete(AbstractDurablePersistence.CNS_KEYSPACE, columnFamilyTopicStats, arn,
    // null, CMB_SERIALIZER.STRING_SERIALIZER, CMB_SERIALIZER.STRING_SERIALIZER);
    // cassandraHandler.deleteCounter(AbstractDurablePersistence.CNS_KEYSPACE,
    // columnFamilyTopicStats, topicArn, "subscriptionConfirmed", CMB_SERIALIZER.STRING_SERIALIZER,
    // CMB_SERIALIZER.STRING_SERIALIZER);
    // cassandraHandler.deleteCounter(AbstractDurablePersistence.CNS_KEYSPACE,
    // columnFamilyTopicStats, topicArn, "subscriptionPending", CMB_SERIALIZER.STRING_SERIALIZER,
    // CMB_SERIALIZER.STRING_SERIALIZER);
    // cassandraHandler.deleteCounter(AbstractDurablePersistence.CNS_KEYSPACE,
    // columnFamilyTopicStats, topicArn, "subscriptionDeleted", CMB_SERIALIZER.STRING_SERIALIZER,
    // CMB_SERIALIZER.STRING_SERIALIZER);

    CNSCache.removeTopic(arn);
  }

  @Override
  public long getNumberOfTopicsByUser(String userId) throws PersistenceException {

    if (userId == null || userId.trim().length() == 0) {
      logger.error("event=list_queues error_code=invalid_user user_id=" + userId);
      throw new PersistenceException(
          CQSErrorCodes.InvalidParameterValue, "Invalid userId " + userId);
    }

    String lastArn = null;
    int sliceSize;
    long numTopics = 0;

    do {

      sliceSize = 0;

      CmbColumnSlice<String, String> slice =
          cassandraHandler.readColumnSlice(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilyTopicsByUserId,
              userId,
              lastArn,
              null,
              10000,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);

      if (slice != null && slice.getColumns().size() > 0) {
        sliceSize = slice.getColumns().size();
        numTopics += sliceSize;
        lastArn = slice.getColumns().get(sliceSize - 1).getName();
      }

    } while (sliceSize >= 10000);

    return numTopics;
  }

  @Override
  public List<CNSTopic> listTopics(String userId, String nextToken) throws Exception {

    if (nextToken != null) {
      if (getTopic(nextToken) == null) {
        nextToken = null;
        // throw new CMBException(CMBErrorCodes.InvalidParameterValue, "Invalid parameter
        // nextToken");
      }
    }

    List<CNSTopic> topics = new ArrayList<CNSTopic>();
    CmbColumnSlice<String, String> slice =
        cassandraHandler.readColumnSlice(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopicsByUserId,
            userId,
            nextToken,
            null,
            100,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (slice != null) {

      for (CmbColumn<String, String> c : slice.getColumns()) {

        String arn = c.getName();

        slice =
            cassandraHandler.readColumnSlice(
                AbstractDurablePersistence.CNS_KEYSPACE,
                columnFamilyTopics,
                arn,
                null,
                null,
                100,
                CMB_SERIALIZER.STRING_SERIALIZER,
                CMB_SERIALIZER.STRING_SERIALIZER,
                CMB_SERIALIZER.STRING_SERIALIZER);

        if (slice != null) {

          String name = slice.getColumnByName("name").getValue();

          String displayName = null;

          if (slice.getColumnByName("displayName") != null) {
            displayName = slice.getColumnByName("displayName").getValue();
          }

          CNSTopic t = new CNSTopic(arn, name, displayName, userId);

          topics.add(t);

        } else {
          cassandraHandler.delete(
              AbstractDurablePersistence.CNS_KEYSPACE,
              columnFamilyTopicsByUserId,
              userId,
              arn,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);
        }
      }
    }

    return topics;
  }

  @Override
  public CNSTopic getTopic(String arn) throws Exception {

    CNSTopic topic = null;
    CmbColumnSlice<String, String> slice =
        cassandraHandler.readColumnSlice(
            AbstractDurablePersistence.CNS_KEYSPACE,
            columnFamilyTopics,
            arn,
            null,
            null,
            10,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (slice != null) {

      String name = slice.getColumnByName("name").getValue();
      String displayName = null;

      if (slice.getColumnByName("displayName") != null) {
        displayName = slice.getColumnByName("displayName").getValue();
      }

      String user = slice.getColumnByName("userId").getValue();
      topic = new CNSTopic(arn, name, displayName, user);
    }

    return topic;
  }

  @Override
  public void updateTopicDisplayName(String arn, String displayName) throws Exception {

    CNSTopic topic = getTopic(arn);

    if (topic != null) {
      topic.setDisplayName(displayName);
      topic.checkIsValid();
      cassandraHandler.insertRow(
          AbstractDurablePersistence.CNS_KEYSPACE,
          topic.getArn(),
          columnFamilyTopics,
          getColumnValues(topic),
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          null);
    }

    CNSCache.removeTopic(arn);
  }
}
