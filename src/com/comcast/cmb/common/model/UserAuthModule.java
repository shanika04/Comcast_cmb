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
package com.comcast.cmb.common.model;

import com.comcast.cmb.common.persistence.IUserPersistence;
import com.comcast.cmb.common.persistence.PersistenceFactory;
import com.comcast.cmb.common.util.AuthUtil;
import com.comcast.cmb.common.util.AuthenticationException;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.ExpiringCache;
import com.comcast.cmb.common.util.ExpiringCache.CacheFullException;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;

/**
 * Provide user authentication capability
 *
 * @author bwolf, aseem, michael, vvenkatraman
 */
public class UserAuthModule implements IAuthModule {

  private IUserPersistence userPersistence;
  private static ExpiringCache<String, User> userCacheByAccessKey =
      new ExpiringCache<String, User>(CMBProperties.getInstance().getUserCacheSizeLimit());
  private static ExpiringCache<String, User> userCacheByUserId =
      new ExpiringCache<String, User>(CMBProperties.getInstance().getUserCacheSizeLimit());

  private static final Logger logger = Logger.getLogger(UserAuthModule.class);

  private static final List<String> ADMIN_ACTIONS =
      Arrays.asList(
          new String[] {
            "HealthCheck", "ManageService", "GetAPIStats", "GetWorkerStats", "ManageWorker"
          });

  public class UserCallableByAccessKey implements Callable<User> {
    String accessKey = null;

    public UserCallableByAccessKey(String k) {
      this.accessKey = k;
    }

    @Override
    public User call() throws Exception {
      User u = userPersistence.getUserByAccessKey(accessKey);
      return u;
    }
  }

  public class UserCallableByUserId implements Callable<User> {
    String userId = null;

    public UserCallableByUserId(String k) {
      this.userId = k;
    }

    @Override
    public User call() throws Exception {
      User u = userPersistence.getUserById(userId);
      return u;
    }
  }

  @Override
  public void setUserPersistence(IUserPersistence up) {
    userPersistence = up;
  }

  private Map<String, String> getAllParameters(HttpServletRequest requestUrl) {

    Enumeration<String> enumeration = requestUrl.getParameterNames();
    Map<String, String> parameters = new HashMap<String, String>();

    while (enumeration.hasMoreElements()) {
      String name = enumeration.nextElement();
      parameters.put(name, requestUrl.getParameter(name));
    }

    return parameters;
  }

  private Map<String, String> getAllHeaders(HttpServletRequest requestUrl) {

    Enumeration<String> enumeration = requestUrl.getHeaderNames();
    Map<String, String> parameters = new HashMap<String, String>();

    while (enumeration.hasMoreElements()) {
      String name = enumeration.nextElement();
      parameters.put(name, requestUrl.getHeader(name));
    }

    return parameters;
  }

  @Override
  public User authenticateByRequest(HttpServletRequest request) throws CMBException {

    Map<String, String> parameters = getAllParameters(request);

    // sample header

    // content-type=application/x-www-form-urlencoded;
    // charset=utf-8
    // x-amz-date=20130109T230435Z
    // connection=Keep-Alive
    // host=localhost:7070
    // content-length=36
    // user-agent=aws-sdk-java/1.3.27 Mac_OS_X/10.7 Java_HotSpot(TM)_64-Bit_Server_VM/20.12-b01-434
    // authorization=AWS4-HMAC-SHA256
    // Credential=50AL8M5BLRQB4LG5MU1C/20130109/us-east-1/us-east-1/aws4_request
    // SignedHeaders=host;user-agent;x-amz-content-sha256;x-amz-date
    // Signature=f4afd88c15fc41aacae2dc8b7d014673a0a51b4bfc7f2932993329be65c3f2fd
    // x-amz-content-sha256=48a38266faf90970d6c7fea9b15e6ba366e5f6397c2970fc893f8a7b5e207bd0

    String accessKey = parameters.get("AWSAccessKeyId");
    String authorizationHeader = request.getHeader("authorization");
    Map<String, String> headers = getAllHeaders(request);

    if (accessKey == null && authorizationHeader != null) {

      if (authorizationHeader.contains("Credential=") && authorizationHeader.contains("/")) {

        accessKey =
            authorizationHeader.substring(
                authorizationHeader.indexOf("Credential=") + "Credential=".length(),
                authorizationHeader.indexOf("/"));
      }
    }

    if (accessKey == null) {
      logger.error("event=authenticate error_code=missing_access_key");
      throw new AuthenticationException(CMBErrorCodes.InvalidAccessKeyId, "No access key provided");
    }

    User user = null;

    try {

      try {
        user =
            userCacheByAccessKey.getAndSetIfNotPresent(
                accessKey,
                new UserCallableByAccessKey(accessKey),
                CMBProperties.getInstance().getUserCacheExpiring() * 1000);
      } catch (CacheFullException e) {
        user = new UserCallableByAccessKey(accessKey).call();
      }

      if (user == null) {
        logger.error(
            "event=authenticate access_key=" + accessKey + " error_code=invalid_accesskey");
        throw new AuthenticationException(
            CMBErrorCodes.InvalidAccessKeyId, "AccessKey " + accessKey + " is not valid");
      }

    } catch (Exception ex) {
      logger.error("event=authenticate", ex);
      throw new AuthenticationException(
          CMBErrorCodes.InvalidAccessKeyId, "AccessKey " + accessKey + " is not valid");
    }

    // admin actions do not require signatures but can only be performed by admin user

    if (ADMIN_ACTIONS.contains(parameters.get("Action"))) {
      if (CMBProperties.getInstance().getCNSUserName().equals(user.getUserName())) {
        logger.debug("event=authenticate action=admin_action");
        return user;
      } else {
        logger.error("event=authenticate error_code=regular_user_attempted_admin_op");
        throw new AuthenticationException(
            CMBErrorCodes.InvalidAccessKeyId, "User not authorized to perform admin actions");
      }
    }

    if ((!CMBProperties.getInstance().getEnableSignatureAuth())
        || (request.getMethod().equals("GET")
            && CMBProperties.getInstance().getAllowGetRequest())) {
      if (!user.getUserName().equals(CMBProperties.getInstance().getCNSUserName())) {
        logger.debug("event=authenticate verify_signature=not_required");
      }
      return user;
    }

    // version 1 and 2 is from parameters
    String version = parameters.get("SignatureVersion");

    // version 4 is recommended from header
    if ((version == null) && (authorizationHeader != null)) {
      if (authorizationHeader.trim().startsWith("AWS4")) {
        version = "4";
      }
    }

    if (!version.equals("1") && !version.equals("2") && !version.equals("4")) {
      logger.error(
          "event=authenticate signature_version="
              + version
              + " error_code=unsupported_signature_version");
      throw new AuthenticationException(
          CMBErrorCodes.NoSuchVersion, "SignatureVersion=" + version + " is not valid");
    }

    // validate signature for version 1 and 2
    if (version.equals("1") || version.equals("2")) {
      String signatureToCheck = parameters.get("Signature");

      if (signatureToCheck == null) {
        logger.error("event=authenticate error_code=no_signature_provided");
        throw new AuthenticationException(CMBErrorCodes.MissingParameter, "Signature not found");
      }

      String timeStamp = parameters.get("Timestamp");
      String expiration = parameters.get("Expires");

      if (timeStamp != null) {
        AuthUtil.checkTimeStamp(timeStamp);
      } else if (expiration != null) {
        AuthUtil.checkExpiration(expiration);
      } else {
        logger.error("event=authenticate error_code=no_time_stamp_or_expiration");
        throw new AuthenticationException(
            CMBErrorCodes.MissingParameter,
            "Request must provide either Timestamp or Expires parameter");
      }

      String signatureMethod = parameters.get("SignatureMethod");

      if (!signatureMethod.equals("HmacSHA256") && !signatureMethod.equals("HmacSHA1")) {
        logger.error(
            "event=authenticate signature_method="
                + signatureMethod
                + " error_code=unsupported_signature_method");
        throw new AuthenticationException(
            CMBErrorCodes.InvalidParameterValue,
            "Signature method " + signatureMethod + " is not supported");
      }

      URL url = null;
      String signature = null;

      try {
        url = new URL(request.getRequestURL().toString());
        parameters.remove("Signature");
        signature =
            AuthUtil.generateSignature(
                url, parameters, version, signatureMethod, user.getAccessSecret());
      } catch (Exception ex) {
        logger.error("event=authenticate url=" + url + " error_code=invalid_url");
        throw new AuthenticationException(CMBErrorCodes.InternalError, "Invalid Url " + url);
      }

      if (signature == null || !signature.equals(signatureToCheck)) {
        logger.error(
            "event=authenticate signature_calculated="
                + signature
                + " signature_given="
                + signatureToCheck
                + " error_code=signature_mismatch");
        throw new AuthenticationException(CMBErrorCodes.InvalidSignature, "Invalid signature");
      }
    }

    // validate signature for version 4
    if (version.equals("4")) {
      // get the signature from head
      String signatureToCheck =
          authorizationHeader.substring(
              authorizationHeader.indexOf("Signature=") + "Signature=".length());

      if (signatureToCheck == null) {
        logger.error("event=authenticate error_code=no_signature_provided");
        throw new AuthenticationException(CMBErrorCodes.MissingParameter, "Signature not found");
      }

      String timeStamp = request.getHeader("X-Amz-Date");

      if (timeStamp != null) {
        AuthUtil.checkTimeStampV4(timeStamp);
      } else {
        logger.error("event=authenticate error_code=no_time_stamp_or_expiration");
        throw new AuthenticationException(
            CMBErrorCodes.MissingParameter,
            "Request must provide either Timestamp or Expires parameter");
      }

      String signatureMethod =
          authorizationHeader
              .substring("AWS4-".length(), authorizationHeader.indexOf("Credential="))
              .trim();
      // currently support HMAC-SHA256
      if (!signatureMethod.equals("HMAC-SHA256")) {
        logger.error(
            "event=authenticate signature_method="
                + signatureMethod
                + " error_code=unsupported_signature_method");
        throw new AuthenticationException(
            CMBErrorCodes.InvalidParameterValue,
            "Signature method " + signatureMethod + " is not supported");
      }

      URL url = null;
      String signature = null;

      try {
        String urlOriginal = request.getRequestURL().toString();
        if (urlOriginal == null || urlOriginal.length() == 0) {
          urlOriginal = "/";
        }
        url = new URL(urlOriginal);
        signature =
            AuthUtil.generateSignatureV4(
                request,
                url,
                parameters,
                headers,
                version,
                signatureMethod,
                user.getAccessSecret());
      } catch (Exception ex) {
        logger.error("event=authenticate url=" + url + " error_code=invalid_url");
        throw new AuthenticationException(CMBErrorCodes.InternalError, "Invalid Url " + url);
      }

      if (signature == null || !signature.equals(signatureToCheck)) {
        logger.error(
            "event=authenticate signature_calculated="
                + signature
                + " signature_given="
                + signatureToCheck
                + " error_code=signature_mismatch");
        throw new AuthenticationException(CMBErrorCodes.InvalidSignature, "Invalid signature");
      }
    }
    logger.debug("event=authenticated_by_signature username=" + user.getUserName());

    return user;
  }

  @Override
  public User authenticateByPassword(String username, String password) throws CMBException {

    try {

      User user = userPersistence.getUserByName(username);

      if (user == null) {
        logger.error(
            "event=authenticate_by_password user=" + username + " error_code=user_not_found");
        throw new AuthenticationException(
            CMBErrorCodes.AuthFailure, "User " + username + " not found");
      }

      if (!AuthUtil.verifyPassword(password, user.getHashPassword())) {
        logger.error(
            "event=authenticate_by_password user=" + username + " error_code=invalid_password");
        throw new AuthenticationException(
            CMBErrorCodes.AuthFailure, "Invalid password for user " + username);
      }

      logger.debug("event=authenticated_by_password username=" + username);

      return user;

    } catch (Exception ex) {
      logger.error("event=authenticate_by_password username=" + username, ex);
      throw new AuthenticationException(
          CMBErrorCodes.AuthFailure, "User " + username + " not found");
    }
  }

  public User getUserByRequest(HttpServletRequest request) {
    Map<String, String> parameters = getAllParameters(request);
    String authorizationHeader = request.getHeader("authorization");
    String accessKey = parameters.get("AWSAccessKeyId");
    if (accessKey == null && authorizationHeader != null) {

      if (authorizationHeader.contains("Credential=") && authorizationHeader.contains("/")) {

        accessKey =
            authorizationHeader.substring(
                authorizationHeader.indexOf("Credential=") + "Credential=".length(),
                authorizationHeader.indexOf("/"));
      }
    }
    if (accessKey == null) {
      return null;
    }
    User user = null;

    try {

      try {
        user =
            userCacheByAccessKey.getAndSetIfNotPresent(
                accessKey,
                new UserCallableByAccessKey(accessKey),
                CMBProperties.getInstance().getUserCacheExpiring() * 1000);
      } catch (CacheFullException e) {
        user = new UserCallableByAccessKey(accessKey).call();
      }

      if (user == null) {
        logger.error(
            "event=get_user_by_request access_key=" + accessKey + " error_code=invalid_accesskey");
        throw new AuthenticationException(
            CMBErrorCodes.InvalidAccessKeyId, "AccessKey " + accessKey + " is not valid");
      }

    } catch (Exception ex) {
      logger.error("event=get_user_by_request", ex);
      return null;
    }
    return user;
  }

  public User getUserByUserId(String userId) {
    // set user Persistence
    setUserPersistence(PersistenceFactory.getUserPersistence());

    User user = null;

    try {

      try {
        user =
            userCacheByUserId.getAndSetIfNotPresent(
                userId,
                new UserCallableByUserId(userId),
                CMBProperties.getInstance().getUserCacheExpiring() * 1000);
      } catch (CacheFullException e) {
        user = new UserCallableByAccessKey(userId).call();
      }

      if (user == null) {
        logger.error("event=get_user_by_userid userId=" + userId + " error_code=invalid_userid");
      }

    } catch (Exception ex) {
      logger.error("event=get_user_by_userid", ex);
    }
    return user;
  }
}
