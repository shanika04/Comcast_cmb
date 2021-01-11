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

import com.comcast.cmb.common.util.Util;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.json.JSONObject;

/**
 * Test servlet for endpoint
 *
 * @author bwolf, michael
 */
public class EndpointServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  // keeps last n messages per endpoint

  private static ConcurrentHashMap<String, List<EndpointMessage>> messageMap =
      new ConcurrentHashMap<String, List<EndpointMessage>>();

  // keeps endpoint stats

  private static ConcurrentHashMap<String, EndpointMetrics> metricsMap =
      new ConcurrentHashMap<String, EndpointMetrics>();

  // keeps config information to model certain error behavior

  // private static Map<String, EndpointFailureConfiguration> failureConfigMap = new
  // ConcurrentHashMap<String, EndpointFailureConfiguration>();

  // for each endpoint message a set of message hashes to help identify duplicates

  private static ConcurrentHashMap<String, Map<String, Boolean>> messageHashes =
      new ConcurrentHashMap<String, Map<String, Boolean>>();

  // for each endpoint keeps number of duplciates received (no record = no dups, 1 = 1 dup, ...)

  private static ConcurrentHashMap<String, AtomicInteger> messageDuplicates =
      new ConcurrentHashMap<String, AtomicInteger>();

  private static AtomicInteger globalMessageCounter = new AtomicInteger(0);

  private static final int MAX_MSG_PER_USER = 100;
  private static final String VERSION = CMBControllerServlet.VERSION;

  private static Logger logger = Logger.getLogger(EndpointServlet.class);
  private static final Random rand = new Random();

  /*private class EndpointFailureConfiguration {
  	public int numFailuresBeforeSuccess = 10;
  	public int currentFailureCounter = 0;
  	public int httpErrorCode = 404;
  }*/

  private class EndpointMessage {
    public String host;
    public String id;
    public String recvOn;
    public String url;
    public String method;
    public String msg;
    public String httpHeader;
    // public boolean failure = false;
  }

  private class EndpointMetrics {

    public EndpointMetrics(EndpointMessage msg) {
      startTime = new Date();
      endTime = new Date();
      lastMessageTime = new Date();
      messageCount = 1;
      minWaveLength = Long.MAX_VALUE;
      minLatency = Long.MAX_VALUE;
      lastMessage = msg;
    }

    public Date startTime;
    public Date endTime;
    public long messageCount;
    public long minWaveLength;
    public long maxWaveLength;
    public long averageWaveLength;
    public long totalWaveLength;
    public long lastWaveLength;
    public long messagesPerSecond;
    public long minLatency;
    public long maxLatency;
    public long averageLatency;
    public long totalLatency;
    public long lastLatency;
    public Date lastMessageTime;
    public EndpointMessage lastMessage;
    public List<Integer> timeSeries = new ArrayList<Integer>();
  }

  private String hashMessage(String message) {

    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      digest.update(message.getBytes("UTF-8"));
      byte[] hash = digest.digest();
      return Base64.encodeBase64URLSafeString(hash);
    } catch (Exception e) {
      logger.error("event=hashing_failed", e);
    }

    return null;
  }

  @Override
  public void init() throws ServletException {

    super.init();

    try {
      Util.initLog4jTest();
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String pathInfo = request.getPathInfo();

    if (pathInfo == null) {
      doHelp(request, response);
      return;
    }

    // logger.info("pathInfo="+pathInfo+" ip="+request.getRemoteAddr());

    if (pathInfo.toLowerCase().startsWith("/recv")) {
      doReceiveMessage(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/help")) {
      doHelp(request, response);
      // } else if (pathInfo.toLowerCase().startsWith("/config")) {
      // doConfig(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/clear")) {
      doClear(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/nop")) {
      doAlmostNothing(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/log")) {
      doLogMessage(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/info")) {
      doDisplayMessages(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/token")) {
      doGetToken(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/metrics")) {
      doMetrics(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/chart")) {
      doChart(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/list")) {
      doList(request, response);
    } else {
      doHelp(request, response);
    }
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {

    String pathInfo = request.getPathInfo();

    if (pathInfo == null) {
      doHelp(request, response);
      return;
    }

    // logger.info("pathInfo="+pathInfo+" ip="+request.getRemoteAddr());

    if (pathInfo.toLowerCase().startsWith("/recv")) {
      doReceiveMessage(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/help")) {
      doHelp(request, response);
      // } else if (pathInfo.toLowerCase().startsWith("/config")) {
      // doConfig(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/clear")) {
      doClear(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/nop")) {
      doAlmostNothing(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/log")) {
      doLogMessage(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/info")) {
      doDisplayMessages(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/token")) {
      doGetToken(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/metrics")) {
      doMetrics(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/chart")) {
      doChart(request, response);
    } else if (pathInfo.toLowerCase().startsWith("/list")) {
      doList(request, response);
    } else {
      doHelp(request, response);
    }
  }

  protected void doHelp(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    StringBuffer sb = new StringBuffer();
    sb.append("<h3>EndPoint Help - V " + VERSION + "</h3>");
    sb.append(
        "<b>/recv/id</b> - receive message and calculate metrics on the fly by test id (can be used for up to 2000 msg/sec)<br/>");
    sb.append(
        "<b>/recv/id?errorCode=<errorCode>&chance=<0-1></b> - occasionally respond with an error<br/>");
    sb.append(
        "<b>/recv/id?delayMS=<delayMS>&chance=<0-1></b> - occasionally respond with a delay<br/>");
    sb.append(
        "<b>/recv/id?numResponseBytes=<numResponseBytes>&chance=<0-1></b> - occasionally respond with an error<br/>");
    sb.append("<b>/log/id</b> - receive and do basic logging for message by test id<br/>");
    sb.append("<b>/nop/id</b> - just receive and globally count messages by test id<br/>");
    sb.append(
        "<b><a href=\"clear\">/clear</a></b> - clear all messages, logs and state information<br/>");
    sb.append("<b>/info/id</b> - list recent messages returning http 200 by test id<br/>");
    sb.append("<b>/token/id</b> - obtain sns token by test id<br/>");
    sb.append("<b>/metrics/id</b> - list metrics for test by id<br/>");
    sb.append("<b>/chart/id</b> - render chart for test by id<br/>");
    sb.append("<b><a href=\"list\">/list</a></b> - list information for all test runs<br/>");
    sb.append("<b>/help</b> - this help<br/>");
    sb.append(
        "<b>/config/id?errorCount=10&statusCode=404</b> - set number of failures and error code before successful receipt of messages begins by test id<br/>");

    doOutput(200, response, "Help", sb.toString());
  }

  /*protected void doConfig(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

      String pathInfo = request.getPathInfo();
      String id = pathInfo.substring(pathInfo.lastIndexOf("/")+1);

      EndpointFailureConfiguration failureConfig = new EndpointFailureConfiguration();
  	failureConfigMap.put(id, failureConfig);

      if (request.getParameter("errorCount") != null) {
      	failureConfig.numFailuresBeforeSuccess = Integer.parseInt(request.getParameter("errorCount"));
  	}

      if (request.getParameter("statusCode") != null) {
      	failureConfig.httpErrorCode = Integer.parseInt(request.getParameter("statusCode"));
  	}

      doRawOutput(200, response, "OK");
  }*/

  protected void doClear(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    messageMap = new ConcurrentHashMap<String, List<EndpointMessage>>();
    metricsMap = new ConcurrentHashMap<String, EndpointMetrics>();
    // failureConfigMap = new ConcurrentHashMap<String, EndpointFailureConfiguration>();
    globalMessageCounter = new AtomicInteger(0);
    messageHashes = new ConcurrentHashMap<String, Map<String, Boolean>>();
    messageDuplicates = new ConcurrentHashMap<String, AtomicInteger>();

    doRawOutput(200, response, "OK");
  }

  protected void doAlmostNothing(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    globalMessageCounter.incrementAndGet();
    doRawOutput(200, response, "OK");
  }

  protected void doLogMessage(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    globalMessageCounter.incrementAndGet();
    String msg = parseMessage(request).msg;
    if (msg.length() > 30) {
      msg = msg.substring(0, 29);
    }
    logger.info(
        "event=Endpoint_dolog_Message url="
            + request.getRequestURI()
            + " msg="
            + msg
            + "..."
            + " ts="
            + System.currentTimeMillis());
    doRawOutput(200, response, "OK");
  }

  protected void doGetToken(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String pathInfo = request.getPathInfo();
    String id = pathInfo.substring(pathInfo.lastIndexOf("/") + 1);

    if (id.toLowerCase().startsWith("token")) {
      doOutput(404, response, "EndPoint - GetToken", "Please provide an id");
      return;
    }

    List<EndpointMessage> messages = getMessages(id);

    if (messages == null || messages.size() == 0) {
      doOutput(404, response, "EndPoint - GetToken", "Cannot find token for id=" + id);
      return;
    }

    for (EndpointMessage message : messages) {

      try {
        JSONObject json = new JSONObject(message.msg);
        String token = json.getString("Token");

        if (token == null) {
          continue;
        }

        doOutput(200, response, "EndPoint - GetToken", token);
        return;

      } catch (Exception ex) {
        continue;
      }
    }

    doOutput(404, response, "EndPoint - GetToken", "Cannot find token for id=" + id);
  }

  private synchronized void addMessage(EndpointMessage msg) {

    // add message

    messageMap.putIfAbsent(msg.id, new Vector<EndpointMessage>(MAX_MSG_PER_USER));
    List<EndpointMessage> messages = messageMap.get(msg.id);

    if (messages.size() == MAX_MSG_PER_USER) {
      messages.remove(MAX_MSG_PER_USER - 1); // remove the oldest message
    }

    messages.add(0, msg);

    // add metrics

    metricsMap.putIfAbsent(msg.id, new EndpointMetrics(msg));
    EndpointMetrics metrics = metricsMap.get(msg.id);

    metrics.endTime = new Date();
    metrics.messageCount++;
    metrics.lastWaveLength = metrics.endTime.getTime() - metrics.lastMessageTime.getTime();
    metrics.lastMessageTime = metrics.endTime;
    metrics.lastMessage = msg;
    metrics.totalWaveLength += metrics.lastWaveLength;

    if (metrics.lastWaveLength < metrics.minWaveLength) {
      metrics.minWaveLength = metrics.lastWaveLength;
    }

    if (metrics.lastWaveLength > metrics.maxWaveLength) {
      metrics.maxWaveLength = metrics.lastWaveLength;
    }

    if (metrics.messageCount > 1) {
      metrics.averageWaveLength = metrics.totalWaveLength / (metrics.messageCount - 1);
    }

    if (metrics.endTime.getTime() > metrics.startTime.getTime()) {
      metrics.messagesPerSecond =
          1000 * metrics.messageCount / (metrics.endTime.getTime() - metrics.startTime.getTime());
    }

    String tokens[] = msg.msg.split(";");

    if (tokens.length >= 2) {
      try {
        Date sentTime = new Date(Long.parseLong(tokens[1]));
        metrics.lastLatency = (new Date()).getTime() - sentTime.getTime();
        metrics.totalLatency += metrics.lastLatency;
      } catch (Exception ex) {
        logger.error("exception", ex);
      }
    }

    if (metrics.messageCount > 0) {
      metrics.averageLatency = metrics.totalLatency / metrics.messageCount;
    }

    if (metrics.lastLatency < metrics.minLatency) {
      metrics.minLatency = metrics.lastLatency;
    }

    if (metrics.lastLatency > metrics.maxLatency) {
      metrics.maxLatency = metrics.lastLatency;
    }

    int idx = (int) (metrics.endTime.getTime() - metrics.startTime.getTime()) / 1000;

    while (metrics.timeSeries.size() - 1 < idx) {
      metrics.timeSeries.add(new Integer(0));
    }

    metrics.timeSeries.set(idx, metrics.timeSeries.get(idx) + 1);

    // check for dups

    String message = null;

    if (msg.msg.contains("\"MessageId\":")) {
      message = msg.msg.substring(0, msg.msg.indexOf("\"MessageId\":"));
    } else {
      message = msg.msg;
    }

    String messageHash = hashMessage(message);
    messageHashes.putIfAbsent(msg.id, new ConcurrentHashMap<String, Boolean>());
    ConcurrentHashMap<String, Boolean> hashSet =
        (ConcurrentHashMap<String, Boolean>) messageHashes.get(msg.id);

    if (hashSet.putIfAbsent(messageHash, true) != null) {
      if (messageDuplicates.putIfAbsent(msg.id, new AtomicInteger(1)) != null) {
        messageDuplicates.get(msg.id).incrementAndGet();
      }
    }
  }

  protected void doChart(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String pathInfo = request.getPathInfo();
    String id = pathInfo.substring(pathInfo.lastIndexOf("/") + 1);

    EndpointMetrics metrics = metricsMap.get(id);

    if (metrics != null) {

      byte b[] = generateChart(metrics, id);

      response.setContentLength(b.length);
      response.setContentType("image/jpeg");
      response.getOutputStream().write(b);
      response.flushBuffer();

    } else {
      doOutput(404, response, "EndPoint - Chart", "Page not found");
    }
  }

  protected void doMetrics(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String pathInfo = request.getPathInfo();
    String id = pathInfo.substring(pathInfo.lastIndexOf("/") + 1);

    EndpointMetrics metrics = metricsMap.get(id);

    if (metrics != null) {

      StringBuilder sb = new StringBuilder();

      sb.append("<table>");
      sb.append("<tr><td>Start Time:</td><td>" + metrics.startTime + "</td></tr>");
      sb.append("<tr><td>End Time:</td><td>" + metrics.endTime + "</td></tr>");
      sb.append(
          "<tr><td><b>Messages Per Second:</b></td><td><b>"
              + metrics.messagesPerSecond
              + "</b></td></tr>");
      sb.append(
          "<tr><td><b>Message Count:</b></td><td><b>" + metrics.messageCount + "</b></td></tr>");
      sb.append(
          "<tr><td><b>Avergae Latency:</b></td><td><b>"
              + metrics.averageLatency
              + "</b></td></tr>");
      sb.append("<tr><td>Last Latency:</td><td>" + metrics.lastLatency + "</td></tr>");
      sb.append("<tr><td>Max Latency:</td><td>" + metrics.maxLatency + "</td></tr>");
      sb.append("<tr><td>Min Latency:</td><td>" + metrics.minLatency + "</td></tr>");
      sb.append("<tr><td>Total Latency:</td><td>" + metrics.totalLatency + "</td></tr>");
      sb.append("<tr><td>Avergae Wave Length:</td><td>" + metrics.averageWaveLength + "</td></tr>");
      sb.append("<tr><td>Last Wave Length:</td><td>" + metrics.lastWaveLength + "</td></tr>");
      sb.append("<tr><td>Max Wave Length:</td><td>" + metrics.maxWaveLength + "</td></tr>");
      sb.append("<tr><td>Min Wave Length:</td><td>" + metrics.minWaveLength + "</td></tr>");
      sb.append("<tr><td>Total Wave Length:</td><td>" + metrics.totalWaveLength + "</td></tr>");
      sb.append("<tr><td>Last Receive Time:</td><td>" + metrics.lastMessageTime + "</td></tr>");
      sb.append("<tr><td>Last Messsage:</td><td>" + metrics.lastMessage.msg + "</td></tr>");
      sb.append("<tr><td>Time Series:</td><td>" + metrics.timeSeries + "</td></tr>");
      sb.append("</table>");

      doOutput(200, response, "EndPoint - Metrics", sb.toString());

    } else {
      doOutput(404, response, "EndPoint - Metrics", "Page not found");
    }
  }

  protected void doList(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    Iterator<String> iter = metricsMap.keySet().iterator();

    StringBuilder sb = new StringBuilder();

    if (globalMessageCounter.get() > 0) {
      sb.append("<b>Global Message Count: " + globalMessageCounter.get() + "</b><br/>");
    }

    if (metricsMap.size() == 0) {
      sb.append("no messages<br/>");
    } else {

      sb.append("<table>");
      sb.append(
          "<tr><th>Row</th><th>ID</th><th>Metrics</th><th>Messages</th><th>Chart</th><th>Message Count</th><th>Last Message Received</th><th>Duplicates</th></tr>");

      int rowNum = 1;

      while (iter.hasNext()) {

        String id = iter.next();
        EndpointMetrics metrics = metricsMap.get(id);
        sb.append("<tr>");
        sb.append("<td>" + rowNum + "</td>");
        sb.append("<td>" + id + "</td>");
        sb.append("<td><a href=\"metrics/" + id + "\">metrics</a></td>");
        sb.append("<td><a href=\"info/" + id + "\">messages</a></td>");
        sb.append("<td><a href=\"chart/" + id + "\">chart</a></td>");
        sb.append("<td>" + (metrics.messageCount - 1) + "</td>");
        sb.append("<td>" + metrics.lastMessageTime + "</td>");
        sb.append(
            "<td>"
                + (messageDuplicates.containsKey(id) ? messageDuplicates.get(id) : "")
                + "</td></tr>");
        rowNum++;
      }

      sb.append("</table>");
    }

    doOutput(200, response, "EndPoint - List", sb.toString());
  }

  private List<EndpointMessage> getMessages(String id) {
    return messageMap.get(id);
  }

  private EndpointMessage parseMessage(HttpServletRequest request) throws IOException {

    EndpointMessage msg = new EndpointMessage();

    String pathInfo = request.getPathInfo();
    msg.id = pathInfo.substring(pathInfo.lastIndexOf("/") + 1);

    SimpleDateFormat fmt = new SimpleDateFormat("yy-MM-dd HH:mm:ss");

    msg.host = request.getRemoteAddr() + "/" + request.getRemoteHost();
    msg.recvOn = fmt.format(new Date());
    msg.url = request.getRequestURL().toString();
    msg.method = request.getMethod();
    msg.msg = "";

    if (msg.method.equals("POST")) {

      BufferedReader reader = request.getReader();
      String line;

      while ((line = reader.readLine()) != null) {
        msg.msg += line;
      }
    }

    return msg;
  }

  protected void doReceiveMessage(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    globalMessageCounter.incrementAndGet();

    EndpointMessage msg = parseMessage(request);

    // obey error behavior only after first message (typically subscription confirmation request)
    // was received successfully

    if (request.getParameter("errorCode") != null && messageMap.containsKey(msg.id)) {

      int errorCode = Integer.parseInt(request.getParameter("errorCode"));

      double chance = 1.0;

      if (request.getParameter("chance") != null) {
        chance = Double.parseDouble(request.getParameter("chance"));
      }

      if (rand.nextDouble() <= chance) {
        doOutput(errorCode, response, "Failed", "Failed");
        return;
      }
    }

    if (request.getParameter("delayMS") != null && messageMap.containsKey(msg.id)) {

      long delayMS = Integer.parseInt(request.getParameter("delayMS"));

      double chance = 1.0;

      if (request.getParameter("chance") != null) {
        chance = Double.parseDouble(request.getParameter("chance"));
      }

      if (rand.nextDouble() <= chance) {
        try {
          Thread.sleep(delayMS);
        } catch (Exception ex) {
          throw new ServletException(ex);
        }
      }
    }

    if (request.getParameter("numResponseBytes") != null && messageMap.containsKey(msg.id)) {

      int numResponseBytes = Integer.parseInt(request.getParameter("numResponseBytes"));

      double chance = 1.0;

      if (request.getParameter("chance") != null) {
        chance = Double.parseDouble(request.getParameter("chance"));
      }

      if (rand.nextDouble() <= chance) {
        char[] bytes = new char[numResponseBytes];
        Arrays.fill(bytes, 'A');
        doRawOutput(200, response, bytes);
        return;
      }
    }

    /*if (failureConfigMap.containsKey(msg.id)) {

    	EndpointFailureConfiguration failureConfig = failureConfigMap.get(msg.id);

        logger.info("[doReceiveMessage] currentFailureCounter="+failureConfig.currentFailureCounter+" numFailuresBeforeSuccess="+failureConfig.numFailuresBeforeSuccess);

        if (failureConfig.currentFailureCounter < failureConfig.numFailuresBeforeSuccess) {

    		failureConfig.currentFailureCounter++;
    		msg.failure = true;

            logger.info("[doReceiveMessage] event=failedResponse");
    		doOutput(failureConfig.httpErrorCode, response, "Failed", "Failed");

    		return;
    	}
    }*/

    Enumeration<String> headerNames = request.getHeaderNames();
    String header = "";
    while (headerNames.hasMoreElements()) {
      String headerName = (String) headerNames.nextElement();
      header += headerName + ": " + request.getHeader(headerName) + "\n";
    }
    msg.httpHeader = header;

    addMessage(msg);
    // logger.info("event=received_message status_code=200 msg_id=" + msg.id);
    doOutput(200, response, "Ok", "Ok");
  }

  private String formatMessage(String str) {

    // Escape the "<" and ">" characters
    String fmtMsg = str.replace("<", "&lt;");
    fmtMsg = fmtMsg.replace(">", "&gt;");

    // pretty print json
    try {
      JSONObject json = new JSONObject(fmtMsg);
      fmtMsg = json.toString(2);
    } catch (Exception ex) {
      // not a json string
    }

    return fmtMsg;
  }

  protected void doDisplayMessages(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String pathInfo = request.getPathInfo();
    String id = pathInfo.substring(pathInfo.lastIndexOf("/") + 1);

    if (id.toLowerCase().startsWith("info")) {
      doOutput(404, response, "EndPoint - Display Message", "Please provide an id");
      return;
    }

    List<EndpointMessage> messages = getMessages(id);

    if (messages == null || messages.size() == 0) {
      doOutput(200, response, "EndPoint - Display Messages", "No messages");
      return;
    }

    String showLastStr = request.getParameter("showLast");
    boolean onlyShowLast = (showLastStr != null && showLastStr.equals("true"));

    String output = "<table border=\"1\">";

    synchronized (messages) {
      for (EndpointMessage msg : messages) {

        output += "<tr><td>Time Recv: " + msg.recvOn + "<br/>";
        output += "Url: " + msg.url + "<br/>";
        output += "Method: " + msg.method + "<br/>";
        output += "Host: " + msg.host + "<br/>";
        output += "<b>Header</b><br/>";
        output += msg.httpHeader.replace("\n", "<br/>");

        /*if (msg.failure) {
        	output += "FAILED (HTTP " + failureConfigMap.get(msg.id).httpErrorCode + ")<br/>";
        }*/

        if (msg.msg != null && msg.msg.length() > 0) {
          output += "<pre>" + formatMessage(msg.msg) + "</pre><br/>";
        }

        output += "</td></tr>";

        if (onlyShowLast) {
          doRawOutput(200, response, msg.msg);
          return;
        }
      }
    }

    output += "</table>";

    doOutput(200, response, "EndPoint - Display Messages", output);
  }

  protected void doOutput(int httpCode, HttpServletResponse response, String title, String body)
      throws IOException {

    response.setContentType("text/html");
    response.setStatus(httpCode);

    PrintWriter out = response.getWriter();

    out.println("<html>");
    out.println("<head><title>" + title + "</title></head><body>");
    out.println(body);
    out.println("</body></html>");
  }

  protected void doRawOutput(int httpCode, HttpServletResponse response, String msg)
      throws IOException {

    response.setContentType("text/plain");
    response.setStatus(httpCode);

    PrintWriter out = response.getWriter();
    out.print(msg);
  }

  protected void doRawOutput(int httpCode, HttpServletResponse response, char[] bytes)
      throws IOException {

    response.setContentType("text/plain");
    response.setStatus(httpCode);

    PrintWriter out = response.getWriter();
    out.print(bytes);
  }

  private byte[] generateChart(EndpointMetrics metric, String id) throws IOException {

    XYSeries series = new XYSeries("Test Run");

    for (int i = 0; i < metric.timeSeries.size(); i++) {
      series.add(i, metric.timeSeries.get(i));
    }

    XYSeriesCollection dataset = new XYSeriesCollection(series);

    JFreeChart chart =
        ChartFactory.createXYBarChart(
            "Start: "
                + metric.startTime
                + " End: "
                + metric.endTime
                + " Message Count: "
                + metric.messageCount,
            "Test Second",
            false,
            "Number of Messages",
            dataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false);

    // File file = new File(getServletContext().getRealPath("WEB-INF" + "/" + id + ".jpeg"));
    // ChartUtilities.saveChartAsJPEG(file, chart, 1600, 400);
    // byte b[] = Files.toByteArray(file);
    // return b;

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);
    return bos.toByteArray();
  }
}
