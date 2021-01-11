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

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.model.CMBPolicy;
import com.comcast.cmb.common.model.User;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CMB_SERIALIZER;
import com.comcast.cmb.common.persistence.AbstractDurablePersistence.CmbRow;
import com.comcast.cmb.common.persistence.DurablePersistenceFactory;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBException;
import com.comcast.cns.io.CNSPopulator;
import com.comcast.cns.io.CNSWorkerStatsPopulator;
import com.comcast.cns.model.CNSWorkerStats;
import com.comcast.cns.tools.CNSWorkerMonitorMBean;
import com.comcast.cns.util.CNSErrorCodes;
import com.comcast.cns.util.CNSWorkerStatWrapper;
import java.util.ArrayList;
import java.util.List;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * Subscribe action
 *
 * @author bwolf
 */
public class CNSManageServiceAction extends CNSAction {

  private static Logger logger = Logger.getLogger(CNSManageServiceAction.class);

  public static final String CNS_API_SERVERS = "CNSAPIServers";
  public static final String CNS_WORKERS = "CNSWorkers";

  public CNSManageServiceAction() {
    super("ManageService");
  }

  @Override
  public boolean isActionAllowed(
      User user, HttpServletRequest request, String service, CMBPolicy policy) throws Exception {
    return true;
  }

  /**
   * Manage cns service and workers
   *
   * @param user the user for whom we are subscribing.
   * @param asyncContext
   */
  @Override
  public boolean doAction(User user, AsyncContext asyncContext) throws Exception {

    HttpServletRequest request = (HttpServletRequest) asyncContext.getRequest();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();

    String task = request.getParameter("Task");

    if (task == null || task.equals("")) {
      logger.error("event=cns_manage_service error_code=missing_parameter_task");
      throw new CMBException(CNSErrorCodes.MissingParameter, "Request parameter Task missing.");
    }

    String host = request.getParameter("Host");
    // for some task, Host is mandatory. Check it.
    if (!task.equals("ClearAPIStats")
        && (!task.equals("StartWorker"))
        && (!task.equals("StopWorker"))
        && (host == null || host.equals(""))) {
      logger.error("event=cns_manage_service error_code=missing_parameter_host");
      throw new CMBException(CNSErrorCodes.MissingParameter, "Request parameter Host missing.");
    }

    AbstractDurablePersistence cassandraHandler = DurablePersistenceFactory.getInstance();

    if (task.equals("ClearWorkerQueues")) {

      List<CmbRow<String, String, String>> rows =
          cassandraHandler.readAllRows(
              AbstractDurablePersistence.CNS_KEYSPACE,
              CNS_WORKERS,
              1000,
              10,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER,
              CMB_SERIALIZER.STRING_SERIALIZER);
      List<CNSWorkerStats> statsList = new ArrayList<CNSWorkerStats>();

      if (rows != null) {

        for (CmbRow<String, String, String> row : rows) {

          CNSWorkerStats stats = new CNSWorkerStats();
          stats.setIpAddress(row.getKey());

          if (row.getColumnSlice().getColumnByName("producerTimestamp") != null) {
            stats.setProducerTimestamp(
                Long.parseLong(
                    row.getColumnSlice().getColumnByName("producerTimestamp").getValue()));
          }

          if (row.getColumnSlice().getColumnByName("consumerTimestamp") != null) {
            stats.setConsumerTimestamp(
                Long.parseLong(
                    row.getColumnSlice().getColumnByName("consumerTimestamp").getValue()));
          }

          if (row.getColumnSlice().getColumnByName("jmxport") != null) {
            stats.setJmxPort(
                Long.parseLong(row.getColumnSlice().getColumnByName("jmxport").getValue()));
          }

          if (row.getColumnSlice().getColumnByName("mode") != null) {
            stats.setMode(row.getColumnSlice().getColumnByName("mode").getValue());
          }

          statsList.add(stats);
        }
      }

      for (CNSWorkerStats stats : statsList) {

        if (stats.getIpAddress().equals(host) && stats.getJmxPort() > 0) {

          JMXConnector jmxConnector = null;
          String url = null;

          try {

            long port = stats.getJmxPort();
            url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";

            JMXServiceURL serviceUrl = new JMXServiceURL(url);
            jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);

            MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
            ObjectName cnsWorkerMonitor =
                new ObjectName("com.comcast.cns.tools:type=CNSWorkerMonitorMBean");
            CNSWorkerMonitorMBean mbeanProxy =
                JMX.newMBeanProxy(mbeanConn, cnsWorkerMonitor, CNSWorkerMonitorMBean.class, false);

            mbeanProxy.clearWorkerQueues();

            String res = CNSWorkerStatsPopulator.getGetManageWorkerResponse();
            response.getWriter().println(res);

            return true;

          } finally {

            if (jmxConnector != null) {
              jmxConnector.close();
            }
          }
        }
      }

      throw new CMBException(
          CMBErrorCodes.NotFound, "Cannot clear worker queues: Host " + host + " not found.");

    } else if (task.equals("RemoveWorkerRecord")) {

      cassandraHandler.delete(
          AbstractDurablePersistence.CNS_KEYSPACE,
          CNS_WORKERS,
          host,
          null,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER);
      String out = CNSPopulator.getResponseMetadata();
      writeResponse(out, response);
      return true;

    } else if (task.equals("ClearAPIStats")) {

      CMBControllerServlet.initStats();
      String out = CNSPopulator.getResponseMetadata();
      writeResponse(out, response);
      return true;

    } else if (task.equals("RemoveRecord")) {

      cassandraHandler.delete(
          AbstractDurablePersistence.CNS_KEYSPACE,
          CNS_API_SERVERS,
          host,
          null,
          CMB_SERIALIZER.STRING_SERIALIZER,
          CMB_SERIALIZER.STRING_SERIALIZER);
      String out = CNSPopulator.getResponseMetadata();
      writeResponse(out, response);
      return true;

    } else if (task.equals("StartWorker") || task.equals("StopWorker")) {
      String dataCenter = request.getParameter("DataCenter");
      if (task.equals("StartWorker")) {
        CNSWorkerStatWrapper.startWorkers(dataCenter);
      } else {
        CNSWorkerStatWrapper.stopWorkers(dataCenter);
      }
      String out = CNSPopulator.getResponseMetadata();
      writeResponse(out, response);
      return true;

    } else {
      logger.error(
          "event=cns_manage_service error_code=invalid_task_parameter valid_values=ClearWorkerQueues,RemoveWorkerRecord,RemoveRecord,ClearAPIStats");
      throw new CMBException(
          CNSErrorCodes.InvalidParameterValue,
          "Request parameter Task missing is invalid. Valid values are ClearWorkerQueues, RemoveWorkerRecord, RemoveRecord, ClearAPIStats.");
    }
  }
}
