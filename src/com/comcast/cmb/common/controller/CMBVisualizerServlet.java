package com.comcast.cmb.common.controller;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.DateTickMarkPosition;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.labels.StandardPieSectionLabelGenerator;
import org.jfree.chart.labels.StandardXYItemLabelGenerator;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.StackedXYBarRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.time.Hour;
import org.jfree.data.time.Minute;
import org.jfree.data.time.TimeTableXYDataset;
import org.jfree.ui.RectangleEdge;

public class CMBVisualizerServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    doGet(request, response);
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String pathInfo = request.getPathInfo();

    if (pathInfo == null) {
      return;
    }

    if (pathInfo.toLowerCase().contains("responsetimeimg")) {
      doResponseTimeChart(request, response);
    } else if (pathInfo.toLowerCase().contains("calldistributionimg")) {
      doApiPieChart(request, response);
    } else if (pathInfo.toLowerCase().contains("callcountimg")) {
      doAPICallCountChart(request, response);
    }
  }

  protected void doResponseTimeChart(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    int resolutionMS = 10;
    String action = null;
    boolean redisOnly = false;
    boolean cassandraOnly = false;

    if (request.getParameter("redis") != null) {
      redisOnly = Boolean.parseBoolean(request.getParameter("redis"));
      resolutionMS = 1;
    } else if (request.getParameter("cassandra") != null) {
      cassandraOnly = Boolean.parseBoolean(request.getParameter("cassandra"));
      resolutionMS = 1;
    } else if (request.getParameter("ac") != null) {
      action = request.getParameter("ac");
      resolutionMS = 10;
    } else if (request.getParameter("rs") != null) {
      resolutionMS = Integer.parseInt(request.getParameter("rs"));
    }
    ;

    byte b[] = generateResponseTimeChart(resolutionMS, action, redisOnly, cassandraOnly);

    response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    response.setHeader("Pragma", "no-cache");
    response.setDateHeader("Expires", 0);
    response.setContentLength(b.length);
    response.setContentType("image/jpeg");
    response.getOutputStream().write(b);
    response.flushBuffer();
  }

  protected void doAPICallCountChart(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    byte b[] = generateApiCallCountChart();

    response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    response.setHeader("Pragma", "no-cache");
    response.setDateHeader("Expires", 0);
    response.setContentLength(b.length);
    response.setContentType("image/jpeg");
    response.getOutputStream().write(b);
    response.flushBuffer();
  }

  protected void doApiPieChart(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    byte b[] = generateApiDistributionPieChart();

    response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    response.setHeader("Pragma", "no-cache");
    response.setDateHeader("Expires", 0);
    response.setContentLength(b.length);
    response.setContentType("image/jpeg");
    response.getOutputStream().write(b);
    response.flushBuffer();
  }

  private byte[] generateApiDistributionPieChart() throws IOException {

    DefaultPieDataset piedataset = new DefaultPieDataset();
    Map<String, Long> apiCounts = new HashMap<String, Long>();
    long total = 0;
    String topApi = null;
    long topCount = 0;

    for (String api : CMBControllerServlet.callStats.keySet()) {
      AtomicLong[][] rt = CMBControllerServlet.callResponseTimesByApi.get(api);
      long count = 0;
      for (int i = 0; i < rt.length; i++) {
        for (int k = 0; k < rt[0].length; k++) {
          count += rt[i][k].longValue();
        }
      }
      total += count;
      apiCounts.put(api, count);
      if (count > topCount) {
        topCount = count;
        topApi = api;
      }
    }

    for (String api : apiCounts.keySet()) {
      piedataset.setValue(api, 1.0 * apiCounts.get(api) / total);
    }

    JFreeChart chart =
        ChartFactory.createPieChart("API Call Distribution", piedataset, true, true, false);
    PiePlot pieplot = (PiePlot) chart.getPlot();

    List<Color> palette = new ArrayList<Color>();

    palette.add(new Color(0x660000));
    palette.add(new Color(0x990000));
    palette.add(new Color(0xFF0000));
    palette.add(new Color(0x003300));
    palette.add(new Color(0x006600));
    palette.add(new Color(0x00FF00));
    palette.add(new Color(0x003399));
    palette.add(new Color(0x0066FF));
    palette.add(new Color(0x00CCFF));
    palette.add(new Color(0x330066));
    palette.add(new Color(0x660066));
    palette.add(new Color(0xCCCC33));
    palette.add(new Color(0x990066));
    palette.add(new Color(0xFF99FF));
    palette.add(new Color(0xFF6633));

    int i = 0;
    for (String api : apiCounts.keySet()) {
      pieplot.setSectionPaint(api, palette.get(i % palette.size()));
      i++;
    }

    pieplot.setNoDataMessage("No data available");
    pieplot.setExplodePercent(topApi, 0.20000000000000001D);

    pieplot.setLabelGenerator(new StandardPieSectionLabelGenerator("{0} ({2} percent)"));
    pieplot.setLabelBackgroundPaint(new Color(220, 220, 220));
    pieplot.setLegendLabelToolTipGenerator(
        new StandardPieSectionLabelGenerator("Tooltip for legend item {0}"));
    pieplot.setSimpleLabels(true);
    pieplot.setInteriorGap(0.0D);

    // generate jpeg

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);

    return bos.toByteArray();
  }

  private byte[] generateResponseTimeChart(
      int resolutionMS, String action, boolean showRedisOnly, boolean showCassandraOnly)
      throws IOException {

    String label = "";

    AtomicLong[][] responseTimesMS;

    if (showRedisOnly) {

      label = "Redis Percentiles 1 MS";
      responseTimesMS = CMBControllerServlet.callResponseTimesRedisMS;

    } else if (showCassandraOnly) {

      label = "Cassandra Percentiles 1 MS";
      responseTimesMS = CMBControllerServlet.callResponseTimesCassandraMS;

    } else if (action != null) {

      label = action + " Percentiles 10 MS";
      responseTimesMS = CMBControllerServlet.callResponseTimesByApi.get(action);

    } else {

      if (resolutionMS == 1) {
        label = "API Percentiles 1 MS";
        responseTimesMS = CMBControllerServlet.callResponseTimes1MS;
      } else if (resolutionMS == 100) {
        label = "API Percentiles 100 MS";
        responseTimesMS = CMBControllerServlet.callResponseTimes100MS;
      } else if (resolutionMS == 1000) {
        label = "API Percentiles 1000 MS";
        responseTimesMS = CMBControllerServlet.callResponseTimes1000MS;
      } else {
        label = "API Percentiles 10 MS";
        responseTimesMS = CMBControllerServlet.callResponseTimes10MS;
      }
    }

    // clone and normalize data

    long[][] rt = new long[CMBControllerServlet.NUM_MINUTES][CMBControllerServlet.NUM_BUCKETS];
    long[] totals = new long[CMBControllerServlet.NUM_MINUTES];
    long grandTotal = 0;
    int activeMinutes = 0;

    for (int i = 0; i < CMBControllerServlet.NUM_MINUTES; i++) {
      for (int k = 0; k < CMBControllerServlet.NUM_BUCKETS; k++) {
        rt[i][k] = responseTimesMS[i][k].longValue();
        totals[i] += rt[i][k];
      }
      if (totals[i] > 0) {
        grandTotal += totals[i];
        activeMinutes++;
      }
    }

    for (int i = 0; i < CMBControllerServlet.NUM_MINUTES; i++) {
      for (int k = 0; k < CMBControllerServlet.NUM_BUCKETS; k++) {
        if (totals[i] == 0) {
          rt[i][k] = 0;
        } else {
          rt[i][k] = 100 * rt[i][k] / totals[i];
        }
      }
    }

    // convert data for rendering

    TimeTableXYDataset dataset = new TimeTableXYDataset();

    for (int i = 0; i < CMBControllerServlet.NUM_MINUTES; i++) {
      for (int k = 0; k < CMBControllerServlet.NUM_BUCKETS; k++) {
        dataset.add(new Minute(i, new Hour()), rt[i][k], (resolutionMS * k) + "ms");
      }
    }

    // generate chart

    DateAxis domainAxis = new DateAxis("Minute");
    domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
    domainAxis.setLowerMargin(0.01);
    domainAxis.setUpperMargin(0.01);
    NumberAxis rangeAxis = new NumberAxis("Response Time");
    rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
    rangeAxis.setUpperMargin(0.10); // leave some space for item labels
    StackedXYBarRenderer renderer = new StackedXYBarRenderer(0.15);
    renderer.setDrawBarOutline(true);
    renderer.setBaseItemLabelsVisible(true);
    renderer.setBaseItemLabelGenerator(new StandardXYItemLabelGenerator());

    /*int r=0, g=200, b=0;
    int inc = 200 / CMBControllerServlet.NUM_BUCKETS;

    for (int i=0; i<CMBControllerServlet.NUM_BUCKETS; i++) {
    	renderer.setSeriesPaint(i, new Color(r,g,b));
    	r+=inc;
    	g-=inc;
    }*/

    renderer.setSeriesPaint(0, new Color(47, 133, 18));
    renderer.setSeriesPaint(1, new Color(151, 195, 30));
    renderer.setSeriesPaint(2, new Color(253, 249, 50));
    renderer.setSeriesPaint(3, new Color(253, 191, 35));
    renderer.setSeriesPaint(4, new Color(253, 123, 26));
    renderer.setSeriesPaint(5, new Color(216, 106, 20));
    renderer.setSeriesPaint(6, new Color(181, 97, 28));
    renderer.setSeriesPaint(7, new Color(208, 56, 20));
    renderer.setSeriesPaint(8, new Color(253, 30, 19));
    renderer.setSeriesPaint(9, new Color(120, 54, 210));

    XYPlot plot = new XYPlot(dataset, domainAxis, rangeAxis, renderer);
    String title = "Response Time Percentiles";

    if (activeMinutes > 0) {
      title =
          label
              + " ["
              + grandTotal / (activeMinutes * 60)
              + " call/sec "
              + activeMinutes
              + " mins "
              + grandTotal
              + " calls]";
    }

    JFreeChart chart = new JFreeChart(title, plot);
    chart.removeLegend();
    // chart.addSubtitle(new TextTitle(""));
    LegendTitle legend = new LegendTitle(plot);
    legend.setFrame(new BlockBorder());
    legend.setPosition(RectangleEdge.BOTTOM);
    chart.addSubtitle(legend);

    // generate jpeg

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);
    return bos.toByteArray();
  }

  private byte[] generateApiCallCountChart() throws IOException {

    // convert data

    TimeTableXYDataset dataset = new TimeTableXYDataset();

    for (String ac : CMBControllerServlet.callResponseTimesByApi.keySet()) {
      AtomicLong[][] responseTimesMS = CMBControllerServlet.callResponseTimesByApi.get(ac);
      for (int i = 0; i < CMBControllerServlet.NUM_MINUTES; i++) {
        long total = 0;
        for (int k = 0; k < CMBControllerServlet.NUM_BUCKETS; k++) {
          total += responseTimesMS[i][k].longValue();
        }
        dataset.add(new Minute(i, new Hour()), total, ac);
      }
    }

    // generate chart

    DateAxis domainAxis = new DateAxis("Minute");
    domainAxis.setTickMarkPosition(DateTickMarkPosition.MIDDLE);
    domainAxis.setLowerMargin(0.01);
    domainAxis.setUpperMargin(0.01);
    NumberAxis rangeAxis = new NumberAxis("Calls");
    rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
    rangeAxis.setUpperMargin(0.10); // leave some space for item labels
    StackedXYBarRenderer renderer = new StackedXYBarRenderer(0.15);
    renderer.setDrawBarOutline(true);
    renderer.setBaseItemLabelsVisible(true);
    renderer.setBaseItemLabelGenerator(new StandardXYItemLabelGenerator());

    renderer.setSeriesPaint(0, new Color(0x660000));
    renderer.setSeriesPaint(1, new Color(0x990000));
    renderer.setSeriesPaint(2, new Color(0xFF0000));
    renderer.setSeriesPaint(3, new Color(0x003300));
    renderer.setSeriesPaint(4, new Color(0x006600));
    renderer.setSeriesPaint(5, new Color(0x00FF00));
    renderer.setSeriesPaint(6, new Color(0x003399));
    renderer.setSeriesPaint(7, new Color(0x0066FF));
    renderer.setSeriesPaint(8, new Color(0x00CCFF));
    renderer.setSeriesPaint(9, new Color(0x330066));
    renderer.setSeriesPaint(10, new Color(0x660066));
    renderer.setSeriesPaint(11, new Color(0xCCCC33));
    renderer.setSeriesPaint(12, new Color(0x990066));
    renderer.setSeriesPaint(13, new Color(0xFF99FF));
    renderer.setSeriesPaint(14, new Color(0xFF6633));

    XYPlot plot = new XYPlot(dataset, domainAxis, rangeAxis, renderer);
    String title = "Call Mix";

    JFreeChart chart = new JFreeChart(title, plot);
    chart.removeLegend();
    // chart.addSubtitle(new TextTitle(""));
    LegendTitle legend = new LegendTitle(plot);
    legend.setFrame(new BlockBorder());
    legend.setPosition(RectangleEdge.BOTTOM);
    chart.addSubtitle(legend);

    // generate jpeg

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ChartUtilities.writeChartAsJPEG(bos, chart, 2400, 400);
    return bos.toByteArray();
  }
}
