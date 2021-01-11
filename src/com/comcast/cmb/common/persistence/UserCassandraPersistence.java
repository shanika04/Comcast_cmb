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
package com.comcast.cmb.common.persistence;

import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbColumnSlice;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbRow;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cqs.util.CQSErrorCodes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Represents Cassandra persistence functionality of User Objects
 *
 * @author bwolf, vvenkatraman, baosen, michael, aseem
 */
public class UserCassandraPersistence implements IUserPersistence {

  private static final String ACCESS_KEY = "accessKey";
  private static final String ACCESS_SECRET = "accessSecret";
  private static final String USER_ID = "userId";
  private static final String HASH_PASSWORD = "hashPassword";
  private static final String IS_ADMIN = "isAdmin";
  private static final String USER_DESC = "description";
  // private static final String USER_NAME = "userName";
  private static final String COLUMN_FAMILY_USERS = "Users";
  private static final Logger logger = Logger.getLogger(UserCassandraPersistence.class);

  private static final AbstractDurablePersistence cassandraHandler =
      DurablePersistenceFactory.getInstance();

  public UserCassandraPersistence() {}

  @Override
  public User createUser(String userName, String password) throws PersistenceException {
    return this.createUser(userName, password, false);
  }

  @Override
  public User createUser(String userName, String password, Boolean isAdmin)
      throws PersistenceException {
    return this.createUser(userName, password, isAdmin, "");
  }

  @Override
  public User createUser(String userName, String password, Boolean isAdmin, String description)
      throws PersistenceException {
    User user = null;

    if (userName == null || userName.length() < 0 || userName.length() > 25) {
      logger.error("event=create_user error_code=invalid_user_name user_name=" + userName);
      throw new PersistenceException(CQSErrorCodes.InvalidRequest, "Invalid user name " + userName);
    }

    if (password == null || password.length() < 0 || password.length() > 25) {
      logger.error("event=create_user error_code=invalid_password");
      throw new PersistenceException(CQSErrorCodes.InvalidRequest, "Invalid password");
    }

    if (getUserByName(userName) != null) {
      logger.error("event=create_user error_code=user_already_exists user_name=" + userName);
      throw new PersistenceException(
          CQSErrorCodes.InvalidRequest, "User with user name " + userName + " already exists");
    }

    try {

      String userId = Long.toString(System.currentTimeMillis()).substring(1);

      String hashedPassword = AuthUtil.hashPassword(password);
      String accessSecret = AuthUtil.generateRandomAccessSecret();
      String accessKey = AuthUtil.generateRandomAccessKey();

      user =
          new User(userId, userName, hashedPassword, accessKey, accessSecret, isAdmin, description);

      Map<String, String> userDataMap = new HashMap<String, String>();

      userDataMap.put(USER_ID, user.getUserId());
      // userDataMap.put(USER_NAME, user.getUserName());
      userDataMap.put(HASH_PASSWORD, user.getHashPassword());
      userDataMap.put(ACCESS_SECRET, user.getAccessSecret());
      userDataMap.put(ACCESS_KEY, user.getAccessKey());
      userDataMap.put(IS_ADMIN, user.getIsAdmin().toString());
      userDataMap.put(USER_DESC, user.getDescription());

      cassandraHandler.insertRow(
          AbstractDurablePersistence.CMB_KEYSPACE,
          user.getUserName(),
          COLUMN_FAMILY_USERS,
          userDataMap,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER,
          null);

    } catch (Exception e) {
      logger.error("event=create_user", e);
      throw new PersistenceException(CQSErrorCodes.InvalidRequest, e.getMessage());
    }

    return user;
  }

  @Override
  public void deleteUser(String userName) throws PersistenceException {
    cassandraHandler.delete(
        AbstractDurablePersistence.CMB_KEYSPACE,
        COLUMN_FAMILY_USERS,
        userName,
        null,
        CMB_SERIALIZER.STRING_SERIALIZER,
        CMB_SERIALIZER.STRING_SERIALIZER);
  }

  @Override
  public long getNumUserQueues(String userId) throws PersistenceException {
    return PersistenceFactory.getQueuePersistence().getNumberOfQueuesByUser(userId);
  }

  @Override
  public long getNumUserTopics(String userId) throws PersistenceException {
    return PersistenceFactory.getTopicPersistence().getNumberOfTopicsByUser(userId);
  }

  @Override
  public User getUserById(String userId) throws PersistenceException {

    List<CmbRow<String, String, String>> rows =
        cassandraHandler.readRowsByIndex(
            AbstractDurablePersistence.CMB_KEYSPACE,
            COLUMN_FAMILY_USERS,
            USER_ID,
            userId,
            10,
            10,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (rows == null || rows.size() == 0) {
      return null;
    }

    User user = fillUserFromRow(rows.get(0).getKey(), rows.get(0).getColumnSlice());
    return user;
  }

  @Override
  public User getUserByName(String userName) throws PersistenceException {

    CmbColumnSlice<String, String> slice =
        cassandraHandler.readColumnSlice(
            AbstractDurablePersistence.CMB_KEYSPACE,
            COLUMN_FAMILY_USERS,
            userName,
            null,
            null,
            10,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (slice == null) {
      return null;
    }

    return fillUserFromRow(userName, slice);
  }

  @Override
  public User getUserByAccessKey(String accessKey) throws PersistenceException {

    List<CmbRow<String, String, String>> rows =
        cassandraHandler.readRowsByIndex(
            AbstractDurablePersistence.CMB_KEYSPACE,
            COLUMN_FAMILY_USERS,
            ACCESS_KEY,
            accessKey,
            10,
            10,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);

    if (rows == null || rows.size() == 0) {
      return null;
    }

    User user = fillUserFromRow(rows.get(0).getKey(), rows.get(0).getColumnSlice());

    return user;
  }

  public List<User> getAllUsers() throws PersistenceException {

    List<CmbRow<String, String, String>> rows =
        cassandraHandler.readAllRows(
            AbstractDurablePersistence.CMB_KEYSPACE,
            COLUMN_FAMILY_USERS,
            1000,
            10,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER,
            CMB_SERIALIZER.STRING_SERIALIZER);
    List<User> userList = new ArrayList<User>();

    if (rows == null || rows.size() == 0) {
      return userList;
    }

    for (CmbRow<String, String, String> row : rows) {

      User user = fillUserFromRow(row.getKey(), row.getColumnSlice());

      if (user != null) {
        userList.add(user);
      }
    }

    return userList;
  }

  private User fillUserFromRow(String userName, CmbColumnSlice<String, String> slice) {

    if (slice == null || slice.size() == 0) {
      return null;
    }

    String userId = null;

    if (slice.getColumnByName(USER_ID) != null) {
      userId = slice.getColumnByName(USER_ID).getValue();
    } else {
      return null;
    }

    String accessKey = null;

    if (slice.getColumnByName(ACCESS_KEY) != null) {
      accessKey = slice.getColumnByName(ACCESS_KEY).getValue();
    } else {
      return null;
    }

    String hashPassword = null;

    if (slice.getColumnByName(HASH_PASSWORD) != null) {
      hashPassword = slice.getColumnByName(HASH_PASSWORD).getValue();
    } else {
      return null;
    }

    String accessSecret = null;

    if (slice.getColumnByName(ACCESS_SECRET) != null) {
      accessSecret = slice.getColumnByName(ACCESS_SECRET).getValue();
    } else {
      return null;
    }

    Boolean isAdmin = false;

    if (slice.getColumnByName(IS_ADMIN) != null) {
      isAdmin = Boolean.parseBoolean(slice.getColumnByName(IS_ADMIN).getValue());
    } else if (userName.equals(CMBProperties.getInstance().getCNSUserName())) {
      isAdmin = true;
    } else {
      isAdmin = false;
    }

    String description = "";

    if (slice.getColumnByName(USER_DESC) != null) {
      description = slice.getColumnByName(USER_DESC).getValue();
    } else {
      description = "";
    }

    User user =
        new User(userId, userName, hashPassword, accessKey, accessSecret, isAdmin, description);

    return user;
  }

  @Override
  public User createDefaultUser() throws PersistenceException {
    return createUser(
        CMBProperties.getInstance().getCNSUserName(),
        CMBProperties.getInstance().getCNSUserPassword(),
        true);
  }
}
