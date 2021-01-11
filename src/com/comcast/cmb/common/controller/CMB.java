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
package com.comcast.cmb.common.controller;

import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.Util;
import com.comcast.cns.tools.CNSPublisher;
import com.comcast.cqs.controller.CQSControllerServlet;
import com.comcast.cqs.controller.CQSLongPollReceiver;
import com.comcast.cqs.controller.CQSLongPollSender;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Bootstrap class for cns, cqs service endpoint servlets and cns workers. Servlets will be launched
 * in embedded Jetty instances.
 *
 * @author bwolf
 */
public class CMB {

  private static Logger logger = Logger.getLogger(CMB.class);

  private static final int MAX_REQUEST_LENGTH =
      Math.max(CMBProperties.getInstance().getCNSMaxMessageSize() * 2, 500 * 1024);

  public static volatile Server cqsServer = null;
  public static volatile WebAppContext cqsWebContext = null;
  public static volatile Server cnsServer = null;
  public static volatile WebAppContext cnsWebContext = null;

  public static void main(String argv[]) throws Exception {

    /*ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    handler.setContextPath("/");
    handler.addServlet(CQSControllerServlet.class, "/");
    server.setHandler(handler);*/

    Util.initLog4j();

    // launch cqs service endpoint if enabled

    if (CMBProperties.getInstance().getCQSServiceEnabled()) {

      cqsServer = new Server(CMBProperties.getInstance().getCQSServerPort());
      cqsServer.setAttribute(
          "org.eclipse.jetty.server.Request.maxFormContentSize", MAX_REQUEST_LENGTH);

      QueuedThreadPool cqsThreadPool =
          new QueuedThreadPool(CMBProperties.getInstance().getCqsPoolSize());
      cqsThreadPool.setMaxQueued(CMBProperties.getInstance().getCqsQueueSize());
      cqsServer.setThreadPool(cqsThreadPool);

      cqsWebContext = new WebAppContext();

      cqsWebContext.setDescriptor("config/WEB-INF-CQS/web.xml");
      cqsWebContext.setResourceBase("WebContent");
      cqsWebContext.setServer(cqsServer);
      cqsWebContext.setParentLoaderPriority(true);
      cqsWebContext.setContextPath("/");

      cqsServer.setHandler(cqsWebContext);
      cqsServer.start();

      logger.info(
          "event=launched_cqs_service_endpoint port="
              + CMBProperties.getInstance().getCQSServerPort()
              + " max_request_length="
              + MAX_REQUEST_LENGTH
              + " cl_read="
              + CMBProperties.getInstance().getReadConsistencyLevel()
              + " cl_write="
              + CMBProperties.getInstance().getWriteConsistencyLevel());

      CQSControllerServlet.writeHeartBeat();

      if (CMBProperties.getInstance().isCQSLongPollEnabled()) {
        CQSLongPollReceiver.listen();
        CQSLongPollSender.init();
      }
    }

    // launch cns service endpoint if enabled

    if (CMBProperties.getInstance().getCNSServiceEnabled()) {

      cnsServer = new Server(CMBProperties.getInstance().getCNSServerPort());
      cnsServer.setAttribute(
          "org.eclipse.jetty.server.Request.maxFormContentSize", MAX_REQUEST_LENGTH);

      QueuedThreadPool cnsThreadPool =
          new QueuedThreadPool(CMBProperties.getInstance().getCnsPoolSize());
      cnsThreadPool.setMaxQueued(CMBProperties.getInstance().getCnsQueueSize());
      cnsServer.setThreadPool(cnsThreadPool);

      WebAppContext cnsWebContext = new WebAppContext();

      cnsWebContext.setDescriptor("config/WEB-INF-CNS/web.xml");
      cnsWebContext.setResourceBase("WebContent");
      cnsWebContext.setServer(cnsServer);
      cnsWebContext.setParentLoaderPriority(true);
      cnsWebContext.setContextPath("/");

      cnsServer.setHandler(cnsWebContext);
      cnsServer.start();

      logger.info(
          "event=launched_cns_service_endpoint port="
              + CMBProperties.getInstance().getCNSServerPort()
              + " max_request_length="
              + MAX_REQUEST_LENGTH
              + " cl_read="
              + CMBProperties.getInstance().getReadConsistencyLevel()
              + " cl_write="
              + CMBProperties.getInstance().getWriteConsistencyLevel());
    }

    // launch cns publish worker if enabled

    if (CMBProperties.getInstance().isCNSPublisherEnabled()) {
      Thread.sleep(1000);
      CNSPublisher.start(CMBProperties.getInstance().getCNSPublisherMode());
      logger.info(
          "event=launched_cns_publisher mode=" + CMBProperties.getInstance().getCNSPublisherMode());
    }
  }
}
