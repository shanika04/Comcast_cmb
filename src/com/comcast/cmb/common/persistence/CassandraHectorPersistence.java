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

import com.comcast.cmb.common.controller.CMBControllerServlet;
import com.comcast.cmb.common.util.CMBErrorCodes;
import com.comcast.cmb.common.util.CMBProperties;
import com.comcast.cmb.common.util.PersistenceException;
import com.comcast.cmb.common.util.ValueAccumulator.AccumulatorName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import me.prettyprint.cassandra.connection.DynamicLoadBalancingPolicy;
import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.connection.RoundRobinBalancingPolicy;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.model.MultigetCountQuery;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;
import org.apache.log4j.Logger;

/**
 * This class represents generic functionality for all Cassandra persistence objects
 *
 * @author aseem, bwolf, vvenkatraman, jorge, baosen, michael
 */
public class CassandraHectorPersistence extends AbstractDurablePersistence {

  private static final int hectorPoolSize = CMBProperties.getInstance().getHectorPoolSize();
  private static final String hectorBalancingPolicy =
      CMBProperties.getInstance().getHectorBalancingPolicy();
  private static final Map<String, String> credentials =
      CMBProperties.getInstance().getCassandraCredentials();

  private static Cluster cluster;
  private static Map<String, Keyspace> keyspaces;

  private static Logger logger = Logger.getLogger(CassandraHectorPersistence.class);

  private static class SimpleConsistencyPolicy implements ConsistencyLevelPolicy {

    private final HConsistencyLevel level;

    public SimpleConsistencyPolicy(HConsistencyLevel l) {
      level = l;
    }

    @Override
    public HConsistencyLevel get(OperationType arg0, String arg1) {
      return level;
    }

    @Override
    public HConsistencyLevel get(OperationType arg0) {
      return level;
    }
  }

  private static CassandraHectorPersistence instance;

  public static CassandraHectorPersistence getInstance() {

    if (instance == null) {
      instance = new CassandraHectorPersistence();
    }

    return instance;
  }

  private CassandraHectorPersistence() {
    initPersistence();
  }

  private Keyspace getKeyspace(String keyspace) {
    return keyspaces.get(keyspace);
  }

  private void initPersistence() {

    long ts1 = System.currentTimeMillis();
    logger.info("event=initializing_hector");

    CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();

    cassandraHostConfigurator.setHosts(CLUSTER_URL);
    cassandraHostConfigurator.setMaxActive(hectorPoolSize);
    cassandraHostConfigurator.setCassandraThriftSocketTimeout(
        CMBProperties.getInstance().getCassandraThriftSocketTimeOutMS());

    cassandraHostConfigurator.setAutoDiscoverHosts(
        CMBProperties.getInstance().isHectorAutoDiscovery());
    cassandraHostConfigurator.setAutoDiscoveryDelayInSeconds(
        CMBProperties.getInstance().getHectorAutoDiscoveryDelaySeconds());

    String dataCenter = CMBProperties.getInstance().getCassandraDataCenter();

    if (dataCenter != null && !dataCenter.equals("")) {
      cassandraHostConfigurator.setAutoDiscoveryDataCenter(dataCenter);
    }

    // some other settings we may be interested in down the road, see here for more details:
    // https://github.com/rantav/hector/wiki/User-Guide

    if (hectorBalancingPolicy != null) {
      if (hectorBalancingPolicy.equals("LeastActiveBalancingPolicy")) {
        cassandraHostConfigurator.setLoadBalancingPolicy(new LeastActiveBalancingPolicy());
      } else if (hectorBalancingPolicy.equals("RoundRobinBalancingPolicy")) {
        cassandraHostConfigurator.setLoadBalancingPolicy(
            new RoundRobinBalancingPolicy()); // default
      } else if (hectorBalancingPolicy.equals("DynamicLoadBalancingPolicy")) {
        cassandraHostConfigurator.setLoadBalancingPolicy(new DynamicLoadBalancingPolicy());
      }
    }

    // cassandraHostConfigurator.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_GROW);

    cluster =
        HFactory.getOrCreateCluster(
            AbstractDurablePersistence.CLUSTER_NAME, cassandraHostConfigurator, credentials);
    keyspaces = new HashMap<String, Keyspace>();

    List<String> keyspaceNames = new ArrayList<String>();
    keyspaceNames.add(CMBProperties.getInstance().getCMBKeyspace());
    keyspaceNames.add(CMBProperties.getInstance().getCNSKeyspace());
    keyspaceNames.add(CMBProperties.getInstance().getCQSKeyspace());

    // for now back to one consistency level for everything

    for (String k : keyspaceNames) {
      Keyspace keyspace =
          HFactory.createKeyspace(
              k,
              cluster,
              new SimpleConsistencyPolicy(
                  HConsistencyLevel.valueOf(
                      CMBProperties.getInstance().getWriteConsistencyLevel())));
      keyspaces.put(k, keyspace);
    }

    long ts2 = System.currentTimeMillis();
    CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
  }

  private static Serializer getSerializer(CmbSerializer s) throws PersistenceException {
    if (s instanceof CmbStringSerializer) {
      return StringSerializer.get();
    } else if (s instanceof CmbCompositeSerializer) {
      return CompositeSerializer.get();
    } else if (s instanceof CmbLongSerializer) {
      return LongSerializer.get();
    }

    throw new PersistenceException(CMBErrorCodes.InternalError, "Unknown serializer " + s);
  }

  private static Object getComposite(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof CmbHectorComposite) {
      return ((CmbHectorComposite) o).getComposite();
    }
    return o;
  }

  private static Collection<Object> getComposites(Collection l) {
    Collection<Object> r = new ArrayList<Object>();
    if (l == null) {
      return null;
    }
    for (Object o : l) {
      r.add(getComposite(o));
    }
    return r;
  }

  private static Object getCmbComposite(Object o) {
    if (o == null) {
      return null;
    }
    if (o instanceof Composite) {
      return new CmbHectorComposite((Composite) o);
    }
    return o;
  }

  @Override
  public CmbComposite getCmbComposite(List<?> l) {
    return new CmbHectorComposite(l);
  }

  @Override
  public CmbComposite getCmbComposite(Object... os) {
    return new CmbHectorComposite(os);
  }

  public static class CmbHectorComposite extends CmbComposite {
    private final Composite composite;

    public CmbHectorComposite(List<?> l) {
      composite = new Composite(l);
    }

    public CmbHectorComposite(Object... os) {
      composite = new Composite(os);
    }

    public CmbHectorComposite(Composite composite) {
      this.composite = composite;
    }

    public Composite getComposite() {
      return composite;
    }

    @Override
    public Object get(int i) {
      return composite.get(i);
    }

    @Override
    public String toString() {
      return composite.toString();
    }

    @Override
    public int compareTo(CmbComposite c) {
      return this.composite.compareTo(((CmbHectorComposite) c).getComposite());
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CmbHectorComposite)) {
        return false;
      }
      return this.composite.equals(((CmbHectorComposite) o).getComposite());
    }
  }

  public static class CmbHectorColumn<N, V> extends CmbColumn<N, V> {
    private HColumn<N, V> hectorColumn;

    public CmbHectorColumn(HColumn<N, V> hectorColumn) {
      this.hectorColumn = hectorColumn;
    }

    @Override
    public N getName() {
      return (N) getCmbComposite(hectorColumn.getName());
    }

    @Override
    public V getValue() {
      return hectorColumn.getValue();
    }

    @Override
    public long getClock() {
      return hectorColumn.getClock();
    }
  }

  public static class CmbHectorRow<K, N, V> extends CmbRow<K, N, V> {
    private Row<K, N, V> row;

    public CmbHectorRow(Row<K, N, V> row) {
      this.row = row;
    }

    @Override
    public K getKey() {
      return row.getKey();
    }

    @Override
    public CmbColumnSlice<N, V> getColumnSlice() {
      return new CmbHectorColumnSlice<N, V>(row.getColumnSlice());
    }
  }

  public static class CmbHectorColumnSlice<N, V> extends CmbColumnSlice<N, V> {
    private ColumnSlice<N, V> hectorSlice;
    private List<CmbColumn<N, V>> columns = null;

    public CmbHectorColumnSlice(ColumnSlice<N, V> hectorSlice) {
      this.hectorSlice = hectorSlice;
    }

    @Override
    public CmbHectorColumn<N, V> getColumnByName(N name) {
      if (hectorSlice.getColumnByName(name) != null) {
        return new CmbHectorColumn<N, V>(hectorSlice.getColumnByName(name));
      } else {
        return null;
      }
    }

    private void loadColumns() {
      if (columns == null) {
        columns = new ArrayList<CmbColumn<N, V>>();
        for (HColumn<N, V> c : hectorSlice.getColumns()) {
          columns.add(new CmbHectorColumn<N, V>(c));
        }
      }
    }

    @Override
    public List<CmbColumn<N, V>> getColumns() {
      loadColumns();
      return columns;
    }

    @Override
    public int size() {
      loadColumns();
      return columns.size();
    }
  }

  private <K, N, V> List<CmbRow<K, N, V>> getRows(List<Row<K, N, V>> rows)
      throws PersistenceException {
    List<CmbRow<K, N, V>> l = new ArrayList<CmbRow<K, N, V>>();
    for (Row<K, N, V> r : rows) {
      l.add(new CmbHectorRow<K, N, V>(r));
    }
    return l;
  }

  @Override
  public boolean isAlive() {

    boolean alive = true;

    List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
    List<String> names = new ArrayList<String>();

    for (KeyspaceDefinition k : keyspaces) {
      names.add(k.getName());
    }

    alive &= names.contains(CMBProperties.getInstance().getCQSKeyspace());
    alive &= names.contains(CMBProperties.getInstance().getCNSKeyspace());
    alive &= names.contains(CMBProperties.getInstance().getCMBKeyspace());

    return alive;
  }

  @Override
  public <K, N, V> void update(
      String keyspace,
      String columnFamily,
      K key,
      N column,
      V value,
      CmbSerializer keySerializer,
      CmbSerializer nameSerializer,
      CmbSerializer valueSerializer,
      Integer ttl)
      throws PersistenceException {
    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=update column_family="
            + columnFamily
            + " key="
            + key
            + " column="
            + column
            + " value="
            + value);
    Mutator<K> mutator =
        HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
    HColumn c =
        HFactory.createColumn(
            getComposite(column),
            value,
            getSerializer(nameSerializer),
            getSerializer(valueSerializer));
    if (ttl != null) {
      c.setTtl(ttl);
    }
    mutator.addInsertion(key, columnFamily, c);
    mutator.execute();
    long ts2 = System.currentTimeMillis();
    CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
    CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
  }

  @Override
  public <K, N, V> List<CmbRow<K, N, V>> readAllRows(
      String keyspace,
      String columnFamily,
      int numRows,
      int numCols,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer,
      CmbSerializer valueSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=read_nextn_nonempty_rows cf="
            + columnFamily
            + " num_rows="
            + numRows
            + " num_cols"
            + numCols);

    try {

      RangeSlicesQuery rangeSlicesQuery =
          HFactory.createRangeSlicesQuery(
                  getKeyspace(keyspace),
                  getSerializer(keySerializer),
                  getSerializer(columnNameSerializer),
                  getSerializer(valueSerializer))
              .setColumnFamily(columnFamily)
              .setRange(null, null, false, numCols)
              .setRowCount(numRows);
      // .setKeys(lastKey, null);

      QueryResult<OrderedRows<K, N, V>> result = rangeSlicesQuery.execute();
      OrderedRows<K, N, V> orderedRows = result.get();
      List<Row<K, N, V>> rows = orderedRows.getList();
      return getRows(rows);

    } finally {
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N, V> List<CmbRow<K, N, V>> readRowsByIndex(
      String keyspace,
      String columnFamily,
      N whereColumn,
      V whereValue,
      int numRows,
      int numCols,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer,
      CmbSerializer valueSerializer)
      throws PersistenceException {

    Map<N, V> columnValues = new HashMap<N, V>();
    columnValues.put(whereColumn, whereValue);

    return readRowsByIndices(
        keyspace,
        columnFamily,
        columnValues,
        numRows,
        numCols,
        keySerializer,
        columnNameSerializer,
        valueSerializer);
  }

  @Override
  public <K, N, V> List<CmbRow<K, N, V>> readRowsByIndices(
      String keyspace,
      String columnFamily,
      Map<N, V> columnValues,
      int numRows,
      int numCols,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer,
      CmbSerializer valueSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=read_nextn_rows cf="
            + columnFamily
            + " num_rows="
            + numRows
            + " num_cols="
            + numCols
            + " values="
            + columnValues);

    try {

      List<Row<K, N, V>> rows = new ArrayList<Row<K, N, V>>();

      IndexedSlicesQuery<K, N, V> indexedSlicesQuery =
          HFactory.createIndexedSlicesQuery(
                  getKeyspace(keyspace),
                  getSerializer(keySerializer),
                  getSerializer(columnNameSerializer),
                  getSerializer(valueSerializer))
              .setColumnFamily(columnFamily)
              .setRange(null, null, false, numCols)
              .setRowCount(numRows);

      for (N name : columnValues.keySet()) {
        indexedSlicesQuery.addEqualsExpression(name, columnValues.get(name));
      }

      QueryResult<OrderedRows<K, N, V>> result = indexedSlicesQuery.execute();

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

      OrderedRows<K, N, V> orderedRows = result.get();
      Iterator<Row<K, N, V>> rowsIterator = orderedRows.iterator();

      while (rowsIterator.hasNext()) {

        Row<K, N, V> row = rowsIterator.next();

        if (row.getColumnSlice().getColumns().isEmpty()) {
          continue;
        }

        rows.add(row);
      }

      return getRows(rows);

    } finally {

      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N, V> CmbColumnSlice<N, V> readColumnSlice(
      String keyspace,
      String columnFamily,
      K key,
      N firstColumnName,
      N lastColumnName,
      int numCols,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer,
      CmbSerializer valueSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=read_row cf="
            + columnFamily
            + " key="
            + key
            + " first_col="
            + firstColumnName
            + " last_col="
            + lastColumnName
            + " num_cols="
            + numCols);

    try {

      SliceQuery<K, N, V> sliceQuery =
          HFactory.createSliceQuery(
                  getKeyspace(keyspace),
                  getSerializer(keySerializer),
                  getSerializer(columnNameSerializer),
                  getSerializer(valueSerializer))
              .setColumnFamily(columnFamily)
              .setRange(getComposite(firstColumnName), getComposite(lastColumnName), false, numCols)
              .setKey(key);

      QueryResult<ColumnSlice<N, V>> result = sliceQuery.execute();
      ColumnSlice<N, V> slice = result.get();

      if (slice == null || slice.getColumns() == null || slice.getColumns().isEmpty()) {
        return null;
      }

      return new CmbHectorColumnSlice<N, V>(slice);

    } finally {

      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N, V> CmbColumn<N, V> readColumn(
      String keyspace,
      String columnFamily,
      K key,
      N columnName,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer,
      CmbSerializer valueSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug("event=read_row cf=" + columnFamily + " key=" + key + " col=" + columnName);

    try {

      ColumnQuery<K, N, V> columnQuery =
          HFactory.createColumnQuery(
                  getKeyspace(keyspace),
                  getSerializer(keySerializer),
                  getSerializer(columnNameSerializer),
                  getSerializer(valueSerializer))
              .setColumnFamily(columnFamily)
              .setKey(key)
              .setName(getComposite(columnName));

      QueryResult<HColumn<N, V>> result = columnQuery.execute();

      if (result.get() == null) {
        return null;
      }

      return new CmbHectorColumn<N, V>(result.get());

    } finally {

      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N, V> void insertRow(
      String keyspace,
      K rowKey,
      String columnFamily,
      Map<N, V> columnValues,
      CmbSerializer keySerializer,
      CmbSerializer nameSerializer,
      CmbSerializer valueSerializer,
      Integer ttl)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=insert_row key="
            + rowKey
            + " cf="
            + columnFamily
            + " ttl="
            + (ttl == null ? "null" : ttl));

    try {

      Mutator<K> mutator =
          HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

      for (N name : columnValues.keySet()) {

        HColumn<N, V> col =
            HFactory.createColumn(
                getComposite(name),
                columnValues.get(name),
                getSerializer(nameSerializer),
                getSerializer(valueSerializer));

        if (ttl != null) {
          col.setTtl(ttl);
        }

        mutator.addInsertion(rowKey, columnFamily, col);
      }

      MutationResult result = mutator.execute();

    } finally {

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N, V> void insertRows(
      String keyspace,
      Map<K, Map<N, V>> rowColumnValues,
      String columnFamily,
      CmbSerializer keySerializer,
      CmbSerializer nameSerializer,
      CmbSerializer valueSerializer,
      Integer ttl)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=insert_rows row_column_values="
            + rowColumnValues
            + " cf="
            + columnFamily
            + " ttl="
            + (ttl == null ? "null" : ttl));

    try {

      Mutator<K> mutator =
          HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

      for (K rowKey : rowColumnValues.keySet()) {

        Map<N, V> columnValues = rowColumnValues.get(rowKey);

        for (N name : columnValues.keySet()) {

          HColumn<N, V> col =
              HFactory.createColumn(
                  getComposite(name),
                  columnValues.get(name),
                  getSerializer(nameSerializer),
                  getSerializer(valueSerializer));

          if (ttl != null) {
            col.setTtl(ttl);
          }

          mutator.addInsertion(rowKey, columnFamily, col);
        }
      }

      MutationResult result = mutator.execute();

    } finally {

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N> void delete(
      String keyspace,
      String columnFamily,
      K key,
      N column,
      CmbSerializer keySerializer,
      CmbSerializer columnSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug("event=delete key=" + key + " column=" + column + " cf=" + columnFamily);

    try {

      Mutator<K> mutator =
          HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

      if (column != null) {
        mutator.addDeletion(
            key, columnFamily, getComposite(column), getSerializer(columnSerializer));
      } else {
        mutator.addDeletion(key, columnFamily);
      }

      mutator.execute();

    } finally {

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N> void deleteBatch(
      String keyspace,
      String columnFamily,
      List<K> keyList,
      List<N> columnList,
      CmbSerializer keySerializer,
      CmbSerializer columnSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug("event=delete_batch key_count=" + keyList.size() + " cf=" + columnFamily);

    try {

      Mutator<K> mutator =
          HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));

      if (columnList == null || columnList.isEmpty()) {
        for (int i = 0; i < keyList.size(); i++) {
          mutator.addDeletion(keyList.get(i), columnFamily);
        }
      } else {
        for (int i = 0; i < keyList.size(); i++) {
          mutator.addDeletion(
              keyList.get(i),
              columnFamily,
              getComposite(columnList.get(i)),
              getSerializer(columnSerializer));
        }
      }

      mutator.execute();

    } finally {

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N> int getCount(
      String keyspace,
      String columnFamily,
      K key,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug("event=get_count cf=" + columnFamily + " key=" + key);

    try {

      MultigetCountQuery<K, N> query =
          new MultigetCountQuery<K, N>(
                  getKeyspace(keyspace),
                  getSerializer(keySerializer),
                  getSerializer(columnNameSerializer))
              .setColumnFamily(columnFamily)
              .setKeys(key)
              .setRange(null, null, 2000000000);

      QueryResult<Map<K, Integer>> result = query.execute();

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

      Map<K, Integer> resultRow = result.get();

      if (resultRow.containsKey(key)) {

        int count = resultRow.get(key);
        return count;
      }

      return -1;

    } finally {

      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N> void incrementCounter(
      String keyspace,
      String columnFamily,
      K rowKey,
      String columnName,
      int incrementBy,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=increment_counter cf="
            + columnFamily
            + " key="
            + rowKey
            + " column="
            + columnName
            + " inc="
            + incrementBy);

    try {

      Mutator<K> mutator =
          HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
      mutator.incrementCounter(rowKey, columnFamily, columnName, incrementBy);
      mutator.execute();

    } finally {

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  @Override
  public <K, N> void decrementCounter(
      String keyspace,
      String columnFamily,
      K rowKey,
      String columnName,
      int decrementBy,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=decrement_counter cf="
            + columnFamily
            + " key="
            + rowKey
            + " column="
            + columnName
            + " dec="
            + decrementBy);

    try {

      Mutator<K> mutator =
          HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
      mutator.decrementCounter(rowKey, columnFamily, columnName, decrementBy);
      mutator.execute();

    } finally {

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }

  /*@Override
  public <K, N> void deleteCounter(String keyspace, String columnFamily, K rowKey,
  		N columnName, CmbSerializer keySerializer,
  		CmbSerializer columnNameSerializer) throws PersistenceException {

  	long ts1 = System.currentTimeMillis();
  	logger.debug("event=decrement_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName);

  	try {

  		Mutator<K> mutator = HFactory.createMutator(getKeyspace(keyspace), getSerializer(keySerializer));
  		mutator.deleteCounter(rowKey, columnFamily, columnName, getSerializer(columnNameSerializer));
  		mutator.execute();

  	} finally {

  		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraWrite, 1L);
  		long ts2 = System.currentTimeMillis();
  		CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraTime, (ts2 - ts1));
  	}
  }*/

  @Override
  public <K, N> long getCounter(
      String keyspace,
      String columnFamily,
      K rowKey,
      N columnName,
      CmbSerializer keySerializer,
      CmbSerializer columnNameSerializer)
      throws PersistenceException {

    long ts1 = System.currentTimeMillis();
    logger.debug(
        "event=get_counter cf=" + columnFamily + " key=" + rowKey + " column=" + columnName);

    try {

      CounterQuery<K, N> countQuery =
          HFactory.createCounterColumnQuery(
              getKeyspace(keyspace),
              getSerializer(keySerializer),
              getSerializer(columnNameSerializer));
      countQuery.setColumnFamily(columnFamily).setKey(rowKey).setName(columnName);
      QueryResult<HCounterColumn<N>> result = countQuery.execute();

      CMBControllerServlet.valueAccumulator.addToCounter(AccumulatorName.CassandraRead, 1L);

      if (result.get() == null) {
        return 0;
      }

      return result.get().getValue();

    } finally {
      long ts2 = System.currentTimeMillis();
      CMBControllerServlet.valueAccumulator.addToCounter(
          AccumulatorName.CassandraTime, (ts2 - ts1));
    }
  }
}
