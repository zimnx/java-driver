/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2021 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.SchemaElement.AGGREGATE;
import static com.datastax.driver.core.SchemaElement.FUNCTION;
import static com.datastax.driver.core.SchemaElement.KEYSPACE;
import static com.datastax.driver.core.SchemaElement.TABLE;
import static com.datastax.driver.core.SchemaElement.TYPE;
import static com.datastax.driver.core.SchemaElement.VIEW;

import com.datastax.driver.core.exceptions.BusyConnectionException;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SchemaParser {

  private static final Logger logger = LoggerFactory.getLogger(SchemaParser.class);

  private static final TypeCodec<List<String>> LIST_OF_TEXT_CODEC =
      TypeCodec.list(TypeCodec.varchar());

  private static final SchemaParser V2_PARSER = new V2SchemaParser();
  private static final SchemaParser V3_PARSER = new V3SchemaParser();
  private static final SchemaParser V4_PARSER = new V4SchemaParser();

  static SchemaParser forVersion(VersionNumber cassandraVersion) {
    if (cassandraVersion.getMajor() >= 4) return V4_PARSER;
    if (cassandraVersion.getMajor() >= 3) return V3_PARSER;
    return V2_PARSER;
  }

  static SchemaParser forDseVersion(VersionNumber dseVersion) {
    if (dseVersion.getMajor() == 6 && dseVersion.getMinor() >= 8) return V4_PARSER;
    if (dseVersion.getMajor() >= 5) return V3_PARSER;
    return V2_PARSER;
  }

  abstract SystemRows fetchSystemRows(
      Cluster cluster,
      SchemaElement targetType,
      String targetKeyspace,
      String targetName,
      List<String> targetSignature,
      Connection connection,
      VersionNumber cassandraVersion)
      throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException;

  abstract String tableNameColumn();

  void refresh(
      Cluster cluster,
      SchemaElement targetType,
      String targetKeyspace,
      String targetName,
      List<String> targetSignature,
      Connection connection,
      VersionNumber cassandraVersion)
      throws ConnectionException, BusyConnectionException, ExecutionException,
          InterruptedException {

    SystemRows rows =
        fetchSystemRows(
            cluster,
            targetType,
            targetKeyspace,
            targetName,
            targetSignature,
            connection,
            cassandraVersion);

    Metadata metadata;
    try {
      metadata = cluster.getMetadata();
    } catch (IllegalStateException e) {
      logger.warn("Unable to refresh metadata, cluster has been closed");
      return;
    }
    metadata.lock.lock();
    try {
      if (targetType == null || targetType == KEYSPACE) {
        // building the whole schema or a keyspace
        assert rows.keyspaces != null;
        Map<String, KeyspaceMetadata> keyspaces = buildKeyspaces(rows, cassandraVersion, cluster);
        updateKeyspaces(metadata, metadata.keyspaces, keyspaces, targetKeyspace);
        // If we rebuild all from scratch or have an updated keyspace, rebuild the token map
        // since some replication on some keyspace may have changed
        metadata.rebuildTokenMap();
      } else {
        assert targetKeyspace != null;
        KeyspaceMetadata keyspace = metadata.keyspaces.get(targetKeyspace);
        // If we update a keyspace we don't know about, something went
        // wrong. Log an error and schedule a full schema rebuild.
        if (keyspace == null) {
          logger.info(
              String.format(
                  "Asked to rebuild %s %s.%s but I don't know keyspace %s",
                  targetType, targetKeyspace, targetName, targetKeyspace));
          metadata.cluster.submitSchemaRefresh(null, null, null, null);
        } else {
          switch (targetType) {
            case TABLE:
              if (rows.tables.containsKey(targetKeyspace)) {
                Map<String, TableMetadata> tables =
                    buildTables(
                        keyspace,
                        rows.tables.get(targetKeyspace),
                        rows.columns.get(targetKeyspace),
                        rows.indexes.get(targetKeyspace),
                        cassandraVersion,
                        cluster);
                updateTables(metadata, keyspace.tables, tables, targetName);
              }
              if (rows.views.containsKey(targetKeyspace)) {
                Map<String, MaterializedViewMetadata> tables =
                    buildViews(
                        keyspace,
                        rows.views.get(targetKeyspace),
                        rows.columns.get(targetKeyspace),
                        cassandraVersion,
                        cluster);
                updateViews(metadata, keyspace.views, tables, targetName);
              }
              break;
            case TYPE:
              if (rows.udts.containsKey(targetKeyspace)) {
                Map<String, UserType> userTypes =
                    buildUserTypes(
                        keyspace, rows.udts.get(targetKeyspace), cassandraVersion, cluster);
                updateUserTypes(metadata, keyspace.userTypes, userTypes, targetName);
              }
              break;
            case FUNCTION:
              if (rows.functions.containsKey(targetKeyspace)) {
                Map<String, FunctionMetadata> functions =
                    buildFunctions(
                        keyspace, rows.functions.get(targetKeyspace), cassandraVersion, cluster);
                updateFunctions(metadata, keyspace.functions, functions, targetName);
              }
              break;
            case AGGREGATE:
              if (rows.aggregates.containsKey(targetKeyspace)) {
                Map<String, AggregateMetadata> aggregates =
                    buildAggregates(
                        keyspace, rows.aggregates.get(targetKeyspace), cassandraVersion, cluster);
                updateAggregates(metadata, keyspace.aggregates, aggregates, targetName);
              }
              break;
          }
        }
      }
    } catch (RuntimeException e) {
      // Failure to parse the schema is definitively wrong so log a full-on error, but this won't
      // generally prevent queries to
      // work and this can happen when new Cassandra versions modify stuff in the schema and the
      // driver hasn't yet be modified.
      // So log, but let things go otherwise.
      logger.error(
          "Error parsing schema from Cassandra system tables: the schema in Cluster#getMetadata() will appear incomplete or stale",
          e);
    } finally {
      metadata.lock.unlock();
    }
  }

  private Map<String, KeyspaceMetadata> buildKeyspaces(
      SystemRows rows, VersionNumber cassandraVersion, Cluster cluster) {

    Map<String, KeyspaceMetadata> keyspaces = new LinkedHashMap<String, KeyspaceMetadata>();
    for (Row keyspaceRow : rows.keyspaces) {
      KeyspaceMetadata keyspace = KeyspaceMetadata.build(keyspaceRow, cassandraVersion);
      Map<String, UserType> userTypes =
          buildUserTypes(keyspace, rows.udts.get(keyspace.getName()), cassandraVersion, cluster);
      for (UserType userType : userTypes.values()) {
        keyspace.add(userType);
      }
      Map<String, TableMetadata> tables =
          buildTables(
              keyspace,
              rows.tables.get(keyspace.getName()),
              rows.columns.get(keyspace.getName()),
              rows.indexes.get(keyspace.getName()),
              cassandraVersion,
              cluster);
      for (TableMetadata table : tables.values()) {
        keyspace.add(table);
      }
      Map<String, FunctionMetadata> functions =
          buildFunctions(
              keyspace, rows.functions.get(keyspace.getName()), cassandraVersion, cluster);
      for (FunctionMetadata function : functions.values()) {
        keyspace.add(function);
      }
      Map<String, AggregateMetadata> aggregates =
          buildAggregates(
              keyspace, rows.aggregates.get(keyspace.getName()), cassandraVersion, cluster);
      for (AggregateMetadata aggregate : aggregates.values()) {
        keyspace.add(aggregate);
      }
      Map<String, MaterializedViewMetadata> views =
          buildViews(
              keyspace,
              rows.views.get(keyspace.getName()),
              rows.columns.get(keyspace.getName()),
              cassandraVersion,
              cluster);
      for (MaterializedViewMetadata view : views.values()) {
        keyspace.add(view);
      }
      keyspaces.put(keyspace.getName(), keyspace);
    }
    if (rows.virtualKeyspaces != null) {
      for (Row keyspaceRow : rows.virtualKeyspaces) {
        KeyspaceMetadata keyspace = KeyspaceMetadata.buildVirtual(keyspaceRow, cassandraVersion);
        Map<String, TableMetadata> tables =
            buildTables(
                keyspace,
                rows.virtualTables.get(keyspace.getName()),
                rows.virtualColumns.get(keyspace.getName()),
                Collections.<String, List<Row>>emptyMap(),
                cassandraVersion,
                cluster);
        for (TableMetadata table : tables.values()) {
          keyspace.add(table);
        }
        keyspaces.put(keyspace.getName(), keyspace);
      }
    }

    return keyspaces;
  }

  protected Map<String, TableMetadata> buildTables(
      KeyspaceMetadata keyspace,
      List<Row> tableRows,
      Map<String, Map<String, ColumnMetadata.Raw>> colsDefs,
      Map<String, List<Row>> indexDefs,
      VersionNumber cassandraVersion,
      Cluster cluster) {
    Map<String, TableMetadata> tables = new LinkedHashMap<String, TableMetadata>();
    if (tableRows != null) {
      for (Row tableDef : tableRows) {
        String cfName = tableDef.getString(tableNameColumn());
        try {
          Map<String, ColumnMetadata.Raw> cols = colsDefs == null ? null : colsDefs.get(cfName);
          if (cols == null || cols.isEmpty()) {
            if (cassandraVersion.getMajor() >= 2) {
              // In C* >= 2.0, we should never have no columns metadata because at the very least we
              // should
              // have the metadata corresponding to the default CQL metadata. So if we don't have
              // any columns,
              // that can only mean that the table got creating concurrently with our schema
              // queries, and the
              // query for columns metadata reached the node before the table was persisted while
              // the table
              // metadata one reached it afterwards. We could make the query to the column metadata
              // sequential
              // with the table metadata instead of in parallel, but it's probably not worth making
              // it slower
              // all the time to avoid this race since 1) it's very very uncommon and 2) we can just
              // ignore the
              // incomplete table here for now and it'll get updated next time with no particular
              // consequence
              // (if the table creation was concurrent with our querying, we'll get a notifciation
              // later and
              // will reupdate the schema for it anyway). See JAVA-320 for why we need this.
              continue;
            } else {
              // C* 1.2 don't persists default CQL metadata, so it's possible not to have columns
              // (for thirft
              // tables). But in that case TableMetadata.build() knows how to handle it.
              cols = Collections.emptyMap();
            }
          }
          List<Row> cfIndexes = (indexDefs == null) ? null : indexDefs.get(cfName);
          TableMetadata table =
              TableMetadata.build(
                  keyspace,
                  tableDef,
                  cols,
                  cfIndexes,
                  tableNameColumn(),
                  cassandraVersion,
                  cluster);
          tables.put(table.getName(), table);
        } catch (RuntimeException e) {
          // See #refresh for why we'd rather not propagate this further
          logger.error(
              String.format(
                  "Error parsing schema for table %s.%s: "
                      + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\") will be missing or incomplete",
                  keyspace.getName(), cfName, keyspace.getName(), cfName),
              e);
        }
      }
    }
    return tables;
  }

  protected Map<String, UserType> buildUserTypes(
      KeyspaceMetadata keyspace,
      List<Row> udtRows,
      VersionNumber cassandraVersion,
      Cluster cluster) {
    Map<String, UserType> userTypes = new LinkedHashMap<String, UserType>();
    if (udtRows != null) {
      for (Row udtRow : maybeSortUdts(udtRows, cluster, keyspace.getName())) {
        UserType type = UserType.build(keyspace, udtRow, cassandraVersion, cluster, userTypes);
        userTypes.put(type.getTypeName(), type);
      }
    }
    return userTypes;
  }

  // Some schema versions require parsing UDTs in a specific order
  protected List<Row> maybeSortUdts(List<Row> udtRows, Cluster cluster, String keyspace) {
    return udtRows;
  }

  protected Map<String, FunctionMetadata> buildFunctions(
      KeyspaceMetadata keyspace,
      List<Row> functionRows,
      VersionNumber cassandraVersion,
      Cluster cluster) {
    Map<String, FunctionMetadata> functions = new LinkedHashMap<String, FunctionMetadata>();
    if (functionRows != null) {
      for (Row functionRow : functionRows) {
        FunctionMetadata function =
            FunctionMetadata.build(keyspace, functionRow, cassandraVersion, cluster);
        if (function != null) {
          String name =
              Metadata.fullFunctionName(function.getSimpleName(), function.getArguments().values());
          functions.put(name, function);
        }
      }
    }
    return functions;
  }

  protected Map<String, AggregateMetadata> buildAggregates(
      KeyspaceMetadata keyspace,
      List<Row> aggregateRows,
      VersionNumber cassandraVersion,
      Cluster cluster) {
    Map<String, AggregateMetadata> aggregates = new LinkedHashMap<String, AggregateMetadata>();
    if (aggregateRows != null) {
      for (Row aggregateRow : aggregateRows) {
        AggregateMetadata aggregate =
            AggregateMetadata.build(keyspace, aggregateRow, cassandraVersion, cluster);
        if (aggregate != null) {
          String name =
              Metadata.fullFunctionName(aggregate.getSimpleName(), aggregate.getArgumentTypes());
          aggregates.put(name, aggregate);
        }
      }
    }
    return aggregates;
  }

  protected Map<String, MaterializedViewMetadata> buildViews(
      KeyspaceMetadata keyspace,
      List<Row> viewRows,
      Map<String, Map<String, ColumnMetadata.Raw>> colsDefs,
      VersionNumber cassandraVersion,
      Cluster cluster) {
    Map<String, MaterializedViewMetadata> views =
        new LinkedHashMap<String, MaterializedViewMetadata>();
    if (viewRows != null) {
      for (Row viewRow : viewRows) {
        String viewName = viewRow.getString("view_name");
        try {
          Map<String, ColumnMetadata.Raw> cols = colsDefs.get(viewName);
          if (cols == null || cols.isEmpty())
            continue; // we probably raced, we will update the metadata next time

          MaterializedViewMetadata view =
              MaterializedViewMetadata.build(keyspace, viewRow, cols, cassandraVersion, cluster);
          if (view != null) views.put(view.getName(), view);
        } catch (RuntimeException e) {
          // See #refresh for why we'd rather not propagate this further
          logger.error(
              String.format(
                  "Error parsing schema for view %s.%s: "
                      + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\") will be missing or incomplete",
                  keyspace.getName(), viewName, keyspace.getName(), viewName),
              e);
        }
      }
    }
    return views;
  }

  // Update oldKeyspaces with the changes contained in newKeyspaces.
  // This method also takes care of triggering the relevant events
  protected void updateKeyspaces(
      Metadata metadata,
      Map<String, KeyspaceMetadata> oldKeyspaces,
      Map<String, KeyspaceMetadata> newKeyspaces,
      String keyspaceToRebuild) {
    Iterator<KeyspaceMetadata> it = oldKeyspaces.values().iterator();
    while (it.hasNext()) {
      KeyspaceMetadata oldKeyspace = it.next();
      String keyspaceName = oldKeyspace.getName();
      // If we're rebuilding only a single keyspace, we should only consider that one
      // because newKeyspaces will only contain that keyspace.
      if ((keyspaceToRebuild == null || keyspaceToRebuild.equals(keyspaceName))
          && !newKeyspaces.containsKey(keyspaceName)) {
        it.remove();
        metadata.triggerOnKeyspaceRemoved(oldKeyspace);
      }
    }
    for (KeyspaceMetadata newKeyspace : newKeyspaces.values()) {
      KeyspaceMetadata oldKeyspace = oldKeyspaces.put(newKeyspace.getName(), newKeyspace);
      if (oldKeyspace == null) {
        metadata.triggerOnKeyspaceAdded(newKeyspace);
      } else if (!oldKeyspace.equals(newKeyspace)) {
        metadata.triggerOnKeyspaceChanged(newKeyspace, oldKeyspace);
      }
      Map<String, TableMetadata> oldTables =
          oldKeyspace == null
              ? new HashMap<String, TableMetadata>()
              : new HashMap<String, TableMetadata>(oldKeyspace.tables);
      updateTables(metadata, oldTables, newKeyspace.tables, null);
      Map<String, UserType> oldTypes =
          oldKeyspace == null
              ? new HashMap<String, UserType>()
              : new HashMap<String, UserType>(oldKeyspace.userTypes);
      updateUserTypes(metadata, oldTypes, newKeyspace.userTypes, null);
      Map<String, FunctionMetadata> oldFunctions =
          oldKeyspace == null
              ? new HashMap<String, FunctionMetadata>()
              : new HashMap<String, FunctionMetadata>(oldKeyspace.functions);
      updateFunctions(metadata, oldFunctions, newKeyspace.functions, null);
      Map<String, AggregateMetadata> oldAggregates =
          oldKeyspace == null
              ? new HashMap<String, AggregateMetadata>()
              : new HashMap<String, AggregateMetadata>(oldKeyspace.aggregates);
      updateAggregates(metadata, oldAggregates, newKeyspace.aggregates, null);
      Map<String, MaterializedViewMetadata> oldViews =
          oldKeyspace == null
              ? new HashMap<String, MaterializedViewMetadata>()
              : new HashMap<String, MaterializedViewMetadata>(oldKeyspace.views);
      updateViews(metadata, oldViews, newKeyspace.views, null);
    }
  }

  private void updateTables(
      Metadata metadata,
      Map<String, TableMetadata> oldTables,
      Map<String, TableMetadata> newTables,
      String tableToRebuild) {
    Iterator<TableMetadata> it = oldTables.values().iterator();
    while (it.hasNext()) {
      TableMetadata oldTable = it.next();
      String tableName = oldTable.getName();
      // If we're rebuilding only a single table, we should only consider that one
      // because newTables will only contain that table.
      if ((tableToRebuild == null || tableToRebuild.equals(tableName))
          && !newTables.containsKey(tableName)) {
        it.remove();
        metadata.triggerOnTableRemoved(oldTable);
      }
    }
    for (TableMetadata newTable : newTables.values()) {
      TableMetadata oldTable = oldTables.put(newTable.getName(), newTable);
      if (oldTable == null) {
        metadata.triggerOnTableAdded(newTable);
      } else {
        // if we're updating a table only, we need to copy views from old table to the new table.
        if (tableToRebuild != null) {
          for (MaterializedViewMetadata view : oldTable.getViews()) {
            view.setBaseTable(newTable);
          }
        }
        if (!oldTable.equals(newTable)) {
          metadata.triggerOnTableChanged(newTable, oldTable);
        }
      }
    }
  }

  private void updateUserTypes(
      Metadata metadata,
      Map<String, UserType> oldTypes,
      Map<String, UserType> newTypes,
      String typeToRebuild) {
    Iterator<UserType> it = oldTypes.values().iterator();
    while (it.hasNext()) {
      UserType oldType = it.next();
      String typeName = oldType.getTypeName();
      if ((typeToRebuild == null || typeToRebuild.equals(typeName))
          && !newTypes.containsKey(typeName)) {
        it.remove();
        metadata.triggerOnUserTypeRemoved(oldType);
      }
    }
    for (UserType newType : newTypes.values()) {
      UserType oldType = oldTypes.put(newType.getTypeName(), newType);
      if (oldType == null) {
        metadata.triggerOnUserTypeAdded(newType);
      } else if (!newType.equals(oldType)) {
        metadata.triggerOnUserTypeChanged(newType, oldType);
      }
    }
  }

  private void updateFunctions(
      Metadata metadata,
      Map<String, FunctionMetadata> oldFunctions,
      Map<String, FunctionMetadata> newFunctions,
      String functionToRebuild) {
    Iterator<FunctionMetadata> it = oldFunctions.values().iterator();
    while (it.hasNext()) {
      FunctionMetadata oldFunction = it.next();
      String oldFunctionName =
          Metadata.fullFunctionName(
              oldFunction.getSimpleName(), oldFunction.getArguments().values());
      if ((functionToRebuild == null || functionToRebuild.equals(oldFunctionName))
          && !newFunctions.containsKey(oldFunctionName)) {
        it.remove();
        metadata.triggerOnFunctionRemoved(oldFunction);
      }
    }
    for (FunctionMetadata newFunction : newFunctions.values()) {
      String newFunctionName =
          Metadata.fullFunctionName(
              newFunction.getSimpleName(), newFunction.getArguments().values());
      FunctionMetadata oldFunction = oldFunctions.put(newFunctionName, newFunction);
      if (oldFunction == null) {
        metadata.triggerOnFunctionAdded(newFunction);
      } else if (!newFunction.equals(oldFunction)) {
        metadata.triggerOnFunctionChanged(newFunction, oldFunction);
      }
    }
  }

  private void updateAggregates(
      Metadata metadata,
      Map<String, AggregateMetadata> oldAggregates,
      Map<String, AggregateMetadata> newAggregates,
      String aggregateToRebuild) {
    Iterator<AggregateMetadata> it = oldAggregates.values().iterator();
    while (it.hasNext()) {
      AggregateMetadata oldAggregate = it.next();
      String oldAggregateName =
          Metadata.fullFunctionName(oldAggregate.getSimpleName(), oldAggregate.getArgumentTypes());
      if ((aggregateToRebuild == null || aggregateToRebuild.equals(oldAggregateName))
          && !newAggregates.containsKey(oldAggregateName)) {
        it.remove();
        metadata.triggerOnAggregateRemoved(oldAggregate);
      }
    }
    for (AggregateMetadata newAggregate : newAggregates.values()) {
      String newAggregateName =
          Metadata.fullFunctionName(newAggregate.getSimpleName(), newAggregate.getArgumentTypes());
      AggregateMetadata oldAggregate = oldAggregates.put(newAggregateName, newAggregate);
      if (oldAggregate == null) {
        metadata.triggerOnAggregateAdded(newAggregate);
      } else if (!newAggregate.equals(oldAggregate)) {
        metadata.triggerOnAggregateChanged(newAggregate, oldAggregate);
      }
    }
  }

  private void updateViews(
      Metadata metadata,
      Map<String, MaterializedViewMetadata> oldViews,
      Map<String, MaterializedViewMetadata> newViews,
      String viewToRebuild) {
    Iterator<MaterializedViewMetadata> it = oldViews.values().iterator();
    while (it.hasNext()) {
      MaterializedViewMetadata oldView = it.next();
      String aggregateName = oldView.getName();
      if ((viewToRebuild == null || viewToRebuild.equals(aggregateName))
          && !newViews.containsKey(aggregateName)) {
        it.remove();
        metadata.triggerOnMaterializedViewRemoved(oldView);
      }
    }
    for (MaterializedViewMetadata newView : newViews.values()) {
      MaterializedViewMetadata oldView = oldViews.put(newView.getName(), newView);
      if (oldView == null) {
        metadata.triggerOnMaterializedViewAdded(newView);
      } else if (!newView.equals(oldView)) {
        metadata.triggerOnMaterializedViewChanged(newView, oldView);
      }
    }
  }

  static Map<String, List<Row>> groupByKeyspace(ResultSet rs) {
    if (rs == null) return Collections.emptyMap();

    Map<String, List<Row>> result = new HashMap<String, List<Row>>();
    for (Row row : rs) {
      String ksName = row.getString(KeyspaceMetadata.KS_NAME);
      List<Row> l = result.get(ksName);
      if (l == null) {
        l = new ArrayList<Row>();
        result.put(ksName, l);
      }
      l.add(row);
    }
    return result;
  }

  static Map<String, Map<String, List<Row>>> groupByKeyspaceAndCf(ResultSet rs, String tableName) {
    if (rs == null) return Collections.emptyMap();

    Map<String, Map<String, List<Row>>> result = Maps.newHashMap();
    for (Row row : rs) {
      String ksName = row.getString(KeyspaceMetadata.KS_NAME);
      String cfName = row.getString(tableName);
      Map<String, List<Row>> rowsByCf = result.get(ksName);
      if (rowsByCf == null) {
        rowsByCf = Maps.newHashMap();
        result.put(ksName, rowsByCf);
      }
      List<Row> l = rowsByCf.get(cfName);
      if (l == null) {
        l = Lists.newArrayList();
        rowsByCf.put(cfName, l);
      }
      l.add(row);
    }
    return result;
  }

  static Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> groupByKeyspaceAndCf(
      ResultSet rs, VersionNumber cassandraVersion, String tableName) {
    if (rs == null) return Collections.emptyMap();

    Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> result =
        new HashMap<String, Map<String, Map<String, ColumnMetadata.Raw>>>();
    for (Row row : rs) {
      String ksName = row.getString(KeyspaceMetadata.KS_NAME);
      String cfName = row.getString(tableName);
      Map<String, Map<String, ColumnMetadata.Raw>> colsByCf = result.get(ksName);
      if (colsByCf == null) {
        colsByCf = new HashMap<String, Map<String, ColumnMetadata.Raw>>();
        result.put(ksName, colsByCf);
      }
      Map<String, ColumnMetadata.Raw> l = colsByCf.get(cfName);
      if (l == null) {
        l = new HashMap<String, ColumnMetadata.Raw>();
        colsByCf.put(cfName, l);
      }
      ColumnMetadata.Raw c = ColumnMetadata.Raw.fromRow(row, cassandraVersion);
      l.put(c.name, c);
    }
    return result;
  }

  private static ResultSetFuture queryAsync(
      String query, Connection connection, ProtocolVersion protocolVersion)
      throws ConnectionException, BusyConnectionException {
    DefaultResultSetFuture future =
        new DefaultResultSetFuture(null, protocolVersion, new Requests.Query(query));
    connection.write(future);
    return future;
  }

  private static ResultSet get(ResultSetFuture future)
      throws InterruptedException, ExecutionException {
    return (future == null) ? null : future.get();
  }

  /**
   * The rows from the system tables that we want to parse to metadata classes. The format of these
   * rows depends on the Cassandra version, but our parsing code knows how to handle the
   * differences.
   */
  private static class SystemRows {
    final ResultSet keyspaces;
    final Map<String, List<Row>> tables;
    final Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> columns;
    final Map<String, List<Row>> udts;
    final Map<String, List<Row>> functions;
    final Map<String, List<Row>> aggregates;
    final Map<String, List<Row>> views;
    final Map<String, Map<String, List<Row>>> indexes;
    final ResultSet virtualKeyspaces;
    final Map<String, List<Row>> virtualTables;
    final Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> virtualColumns;

    public SystemRows(
        ResultSet keyspaces,
        Map<String, List<Row>> tables,
        Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> columns,
        Map<String, List<Row>> udts,
        Map<String, List<Row>> functions,
        Map<String, List<Row>> aggregates,
        Map<String, List<Row>> views,
        Map<String, Map<String, List<Row>>> indexes,
        ResultSet virtualKeyspaces,
        Map<String, List<Row>> virtualTables,
        Map<String, Map<String, Map<String, ColumnMetadata.Raw>>> virtualColumns) {
      this.keyspaces = keyspaces;
      this.tables = tables;
      this.columns = columns;
      this.udts = udts;
      this.functions = functions;
      this.aggregates = aggregates;
      this.views = views;
      this.indexes = indexes;
      this.virtualKeyspaces = virtualKeyspaces;
      this.virtualTables = virtualTables;
      this.virtualColumns = virtualColumns;
    }
  }

  private static class V2SchemaParser extends SchemaParser {

    private static final String SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces";
    private static final String SELECT_COLUMN_FAMILIES =
        "SELECT * FROM system.schema_columnfamilies";
    private static final String SELECT_COLUMNS = "SELECT * FROM system.schema_columns";
    private static final String SELECT_USERTYPES = "SELECT * FROM system.schema_usertypes";
    private static final String SELECT_FUNCTIONS = "SELECT * FROM system.schema_functions";
    private static final String SELECT_AGGREGATES = "SELECT * FROM system.schema_aggregates";

    private static final String CF_NAME = "columnfamily_name";

    @Override
    SystemRows fetchSystemRows(
        Cluster cluster,
        SchemaElement targetType,
        String targetKeyspace,
        String targetName,
        List<String> targetSignature,
        Connection connection,
        VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException,
            InterruptedException {

      boolean isSchemaOrKeyspace = (targetType == null || targetType == KEYSPACE);

      String whereClause = "";
      if (targetType != null) {
        whereClause = " WHERE keyspace_name = '" + targetKeyspace + '\'';
        if (targetType == TABLE) whereClause += " AND columnfamily_name = '" + targetName + '\'';
        else if (targetType == TYPE) whereClause += " AND type_name = '" + targetName + '\'';
        else if (targetType == FUNCTION)
          whereClause +=
              " AND function_name = '"
                  + targetName
                  + "' AND signature = "
                  + LIST_OF_TEXT_CODEC.format(targetSignature);
        else if (targetType == AGGREGATE)
          whereClause +=
              " AND aggregate_name = '"
                  + targetName
                  + "' AND signature = "
                  + LIST_OF_TEXT_CODEC.format(targetSignature);
      }

      ResultSetFuture ksFuture = null,
          udtFuture = null,
          cfFuture = null,
          colsFuture = null,
          functionsFuture = null,
          aggregatesFuture = null;

      ProtocolVersion protocolVersion =
          cluster.getConfiguration().getProtocolOptions().getProtocolVersion();

      if (isSchemaOrKeyspace)
        ksFuture = queryAsync(SELECT_KEYSPACES + whereClause, connection, protocolVersion);

      if (isSchemaOrKeyspace && supportsUdts(cassandraVersion) || targetType == TYPE)
        udtFuture = queryAsync(SELECT_USERTYPES + whereClause, connection, protocolVersion);

      if (isSchemaOrKeyspace || targetType == TABLE) {
        cfFuture = queryAsync(SELECT_COLUMN_FAMILIES + whereClause, connection, protocolVersion);
        colsFuture = queryAsync(SELECT_COLUMNS + whereClause, connection, protocolVersion);
      }

      if ((isSchemaOrKeyspace && supportsUdfs(cassandraVersion) || targetType == FUNCTION))
        functionsFuture = queryAsync(SELECT_FUNCTIONS + whereClause, connection, protocolVersion);

      if (isSchemaOrKeyspace && supportsUdfs(cassandraVersion) || targetType == AGGREGATE)
        aggregatesFuture = queryAsync(SELECT_AGGREGATES + whereClause, connection, protocolVersion);

      return new SystemRows(
          get(ksFuture),
          groupByKeyspace(get(cfFuture)),
          groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, CF_NAME),
          groupByKeyspace(get(udtFuture)),
          groupByKeyspace(get(functionsFuture)),
          groupByKeyspace(get(aggregatesFuture)),
          // No views nor separate indexes table in Cassandra 2:
          Collections.<String, List<Row>>emptyMap(),
          Collections.<String, Map<String, List<Row>>>emptyMap(),
          null,
          Collections.<String, List<Row>>emptyMap(),
          Collections.<String, Map<String, Map<String, ColumnMetadata.Raw>>>emptyMap());
    }

    @Override
    String tableNameColumn() {
      return CF_NAME;
    }

    private boolean supportsUdts(VersionNumber cassandraVersion) {
      return cassandraVersion.getMajor() > 2
          || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1);
    }

    private boolean supportsUdfs(VersionNumber cassandraVersion) {
      return cassandraVersion.getMajor() > 2
          || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 2);
    }
  }

  private static class V3SchemaParser extends SchemaParser {

    protected static final String SELECT_KEYSPACES = "SELECT * FROM system_schema.keyspaces";
    protected static final String SELECT_TABLES = "SELECT * FROM system_schema.tables";
    protected static final String SELECT_COLUMNS = "SELECT * FROM system_schema.columns";
    protected static final String SELECT_USERTYPES = "SELECT * FROM system_schema.types";
    protected static final String SELECT_FUNCTIONS = "SELECT * FROM system_schema.functions";
    protected static final String SELECT_AGGREGATES = "SELECT * FROM system_schema.aggregates";
    protected static final String SELECT_INDEXES = "SELECT * FROM system_schema.indexes";
    protected static final String SELECT_VIEWS = "SELECT * FROM system_schema.views";

    private static final String TABLE_NAME = "table_name";
    private static final String VIEW_NAME = "view_name";
    private static final String COLUMN_NAME = "column_name";
    private static final String INDEX_NAME = "index_name";
    private static final String FUNCTION_NAME = "function_name";
    private static final String ARGUMENT_TYPES = "argument_types";
    private static final String AGGREGATE_NAME = "aggregate_name";
    private static final String TYPE_NAME = "type_name";
    private static final String LIMIT = " LIMIT 1000";

    private List<Row> fetchUDTs(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix = SELECT_USERTYPES + whereClause(KEYSPACE, keyspace.getName(), null, null);
      List<Row> result = new ArrayList<Row>();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        result.addAll(rs);
        String lastSeen = "'" + result.get(result.size() - 1).getString(TYPE_NAME) + "'";
        rs =
            queryAsync(
                    queryPrefix + " AND " + TYPE_NAME + " > " + lastSeen + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private void buildUDTs(
        KeyspaceMetadata keyspace,
        Cluster cluster,
        Connection connection,
        VersionNumber cassandraVersion,
        ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      List<Row> raw = fetchUDTs(keyspace, connection, protocolVersion);
      Map<String, UserType> userTypes = buildUserTypes(keyspace, raw, cassandraVersion, cluster);
      for (UserType userType : userTypes.values()) {
        keyspace.add(userType);
      }
    }

    private List<Row> fetchFunctions(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix = SELECT_FUNCTIONS + whereClause(KEYSPACE, keyspace.getName(), null, null);
      List<Row> result = new ArrayList<Row>();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        String lastSeenFunction = "'" + rs.get(rs.size() - 1).getString(FUNCTION_NAME) + "'";
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (String arg_type : rs.get(rs.size() - 1).getList(ARGUMENT_TYPES, String.class)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append("'").append(arg_type).append("'");
        }
        sb.append("]");
        String lastSeenArgs = sb.toString();
        result.addAll(rs);
        rs =
            queryAsync(
                    queryPrefix
                        + " AND ("
                        + FUNCTION_NAME
                        + ", "
                        + ARGUMENT_TYPES
                        + ") > ("
                        + lastSeenFunction
                        + ", "
                        + lastSeenArgs
                        + ")"
                        + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private void buildFunctions(
        KeyspaceMetadata keyspace,
        Cluster cluster,
        Connection connection,
        VersionNumber cassandraVersion,
        ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      List<Row> raw = fetchFunctions(keyspace, connection, protocolVersion);
      Map<String, FunctionMetadata> functions =
          buildFunctions(keyspace, raw, cassandraVersion, cluster);
      for (FunctionMetadata function : functions.values()) {
        keyspace.add(function);
      }
    }

    private List<Row> fetchAggregates(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix =
          SELECT_AGGREGATES + whereClause(KEYSPACE, keyspace.getName(), null, null);
      List<Row> result = new ArrayList<Row>();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        String lastSeenAggregate = "'" + rs.get(rs.size() - 1).getString(AGGREGATE_NAME) + "'";
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (String arg_type : rs.get(rs.size() - 1).getList(ARGUMENT_TYPES, String.class)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append("'").append(arg_type).append("'");
        }
        sb.append("]");
        String lastSeenArgs = sb.toString();
        result.addAll(rs);
        rs =
            queryAsync(
                    queryPrefix
                        + " AND ("
                        + AGGREGATE_NAME
                        + ", "
                        + ARGUMENT_TYPES
                        + ") > ("
                        + lastSeenAggregate
                        + ", "
                        + lastSeenArgs
                        + ")"
                        + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private void buildAggregates(
        KeyspaceMetadata keyspace,
        Cluster cluster,
        Connection connection,
        VersionNumber cassandraVersion,
        ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      List<Row> raw = fetchAggregates(keyspace, connection, protocolVersion);
      Map<String, AggregateMetadata> aggregates =
          buildAggregates(keyspace, raw, cassandraVersion, cluster);
      for (AggregateMetadata aggregate : aggregates.values()) {
        keyspace.add(aggregate);
      }
    }

    private Map<String, Map<String, Row>> fetchColumns(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix = SELECT_COLUMNS + whereClause(KEYSPACE, keyspace.getName(), null, null);
      Map<String, Map<String, Row>> result = new HashMap<String, Map<String, Row>>();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        String lastSeenTable = "'" + rs.get(rs.size() - 1).getString(TABLE_NAME) + "'";
        String lastSeenColumn = "'" + rs.get(rs.size() - 1).getString(COLUMN_NAME) + "'";
        for (Row row : rs) {
          String cfName = row.getString(TABLE_NAME);
          Map<String, Row> colsByCf = result.get(cfName);
          if (colsByCf == null) {
            colsByCf = new HashMap<String, Row>();
            result.put(cfName, colsByCf);
          }
          colsByCf.put(row.getString(ColumnMetadata.COLUMN_NAME), row);
        }
        rs =
            queryAsync(
                    queryPrefix
                        + " AND ("
                        + TABLE_NAME
                        + ", "
                        + COLUMN_NAME
                        + ") > ("
                        + lastSeenTable
                        + ", "
                        + lastSeenColumn
                        + ")"
                        + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private Map<String, Map<String, ColumnMetadata.Raw>> buildColumns(
        KeyspaceMetadata keyspace,
        Connection connection,
        VersionNumber cassandraVersion,
        ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      Map<String, Map<String, Row>> raw = fetchColumns(keyspace, connection, protocolVersion);
      Map<String, Map<String, ColumnMetadata.Raw>> result =
          new HashMap<String, Map<String, ColumnMetadata.Raw>>();
      for (Entry<String, Map<String, Row>> table : raw.entrySet()) {
        Map<String, ColumnMetadata.Raw> columns = new HashMap<String, ColumnMetadata.Raw>();
        for (Entry<String, Row> column : table.getValue().entrySet()) {
          columns.put(
              column.getKey(), ColumnMetadata.Raw.fromRow(column.getValue(), cassandraVersion));
        }
        result.put(table.getKey(), columns);
      }
      return result;
    }

    private List<Row> fetchTables(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix = SELECT_TABLES + whereClause(KEYSPACE, keyspace.getName(), null, null);
      List<Row> result = new ArrayList<Row>();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        result.addAll(rs);
        String lastSeen = "'" + result.get(result.size() - 1).getString(TABLE_NAME) + "'";
        rs =
            queryAsync(
                    queryPrefix + " AND " + TABLE_NAME + " > " + lastSeen + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private Map<String, List<Row>> fetchIndexes(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix = SELECT_INDEXES + whereClause(KEYSPACE, keyspace.getName(), null, null);
      Map<String, List<Row>> result = Maps.newHashMap();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        String lastSeenTable = "'" + rs.get(rs.size() - 1).getString(TABLE_NAME) + "'";
        String lastSeenIndex = "'" + rs.get(rs.size() - 1).getString(INDEX_NAME) + "'";
        for (Row row : rs) {
          String cfName = row.getString(TABLE_NAME);
          List<Row> rowsByCf = result.get(cfName);
          if (rowsByCf == null) {
            rowsByCf = Lists.newArrayList();
            result.put(cfName, rowsByCf);
          }
          rowsByCf.add(row);
        }
        rs =
            queryAsync(
                    queryPrefix
                        + " AND ("
                        + TABLE_NAME
                        + ", "
                        + INDEX_NAME
                        + ") > ("
                        + lastSeenTable
                        + ", "
                        + lastSeenIndex
                        + ")"
                        + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private List<Row> fetchViews(
        KeyspaceMetadata keyspace, Connection connection, ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      String queryPrefix = SELECT_VIEWS + whereClause(KEYSPACE, keyspace.getName(), null, null);
      List<Row> result = new ArrayList<Row>();
      List<Row> rs = queryAsync(queryPrefix + LIMIT, connection, protocolVersion).get().all();
      while (!rs.isEmpty()) {
        result.addAll(rs);
        String lastSeen = "'" + result.get(result.size() - 1).getString(VIEW_NAME) + "'";
        rs =
            queryAsync(
                    queryPrefix + " AND " + VIEW_NAME + " > " + lastSeen + LIMIT,
                    connection,
                    protocolVersion)
                .get()
                .all();
      }
      return result;
    }

    private void buildTablesIndexesAndViews(
        KeyspaceMetadata keyspace,
        Cluster cluster,
        Connection connection,
        VersionNumber cassandraVersion,
        ProtocolVersion protocolVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      List<Row> cf = fetchTables(keyspace, connection, protocolVersion);
      Map<String, Map<String, ColumnMetadata.Raw>> columns =
          buildColumns(keyspace, connection, cassandraVersion, protocolVersion);
      Map<String, List<Row>> indexes = fetchIndexes(keyspace, connection, protocolVersion);
      Map<String, TableMetadata> tables =
          buildTables(keyspace, cf, columns, indexes, cassandraVersion, cluster);
      for (TableMetadata table : tables.values()) {
        keyspace.add(table);
      }
      List<Row> viewsData = fetchViews(keyspace, connection, protocolVersion);
      Map<String, MaterializedViewMetadata> views =
          buildViews(keyspace, viewsData, columns, cassandraVersion, cluster);
      for (MaterializedViewMetadata view : views.values()) {
        keyspace.add(view);
      }
    }

    private Map<String, KeyspaceMetadata> buildSchema(
        Cluster cluster, Connection connection, VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, InterruptedException,
            ExecutionException {
      ProtocolVersion protocolVersion =
          cluster.getConfiguration().getProtocolOptions().getProtocolVersion();

      Map<String, KeyspaceMetadata> keyspaces = new LinkedHashMap<String, KeyspaceMetadata>();
      ResultSet keyspacesData = queryAsync(SELECT_KEYSPACES, connection, protocolVersion).get();
      for (Row keyspaceRow : keyspacesData) {
        KeyspaceMetadata keyspace = KeyspaceMetadata.build(keyspaceRow, cassandraVersion);
        keyspaces.put(keyspace.getName(), keyspace);
      }

      for (Entry<String, KeyspaceMetadata> keyspace : keyspaces.entrySet()) {
        buildUDTs(keyspace.getValue(), cluster, connection, cassandraVersion, protocolVersion);
        buildFunctions(keyspace.getValue(), cluster, connection, cassandraVersion, protocolVersion);
        buildAggregates(
            keyspace.getValue(), cluster, connection, cassandraVersion, protocolVersion);
        buildTablesIndexesAndViews(
            keyspace.getValue(), cluster, connection, cassandraVersion, protocolVersion);
      }

      return keyspaces;
    }

    @Override
    void refresh(
        Cluster cluster,
        SchemaElement targetType,
        String targetKeyspace,
        String targetName,
        List<String> targetSignature,
        Connection connection,
        VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException,
            InterruptedException {
      if (targetType == null
          && cluster.getConfiguration().getQueryOptions().isSchemaQueriesPaged()) {
        Map<String, KeyspaceMetadata> keyspaces =
            buildSchema(cluster, connection, cassandraVersion);
        Metadata metadata;
        try {
          metadata = cluster.getMetadata();
        } catch (IllegalStateException e) {
          logger.warn("Unable to refresh metadata, cluster has been closed");
          return;
        }
        metadata.lock.lock();
        try {
          updateKeyspaces(metadata, metadata.keyspaces, keyspaces, null);
          // If we rebuild all from scratch or have an updated keyspace, rebuild the token map
          // since some replication on some keyspace may have changed
          metadata.rebuildTokenMap();
        } catch (RuntimeException e) {
          // Failure to parse the schema is definitively wrong so log a full-on error, but this
          // won't
          // generally prevent queries to
          // work and this can happen when new Cassandra versions modify stuff in the schema and the
          // driver hasn't yet be modified.
          // So log, but let things go otherwise.
          logger.error(
              "Error parsing schema from Cassandra system tables: the schema in Cluster#getMetadata() will appear incomplete or stale",
              e);
        } finally {
          metadata.lock.unlock();
        }
      } else {
        super.refresh(
            cluster,
            targetType,
            targetKeyspace,
            targetName,
            targetSignature,
            connection,
            cassandraVersion);
      }
    }

    @Override
    SystemRows fetchSystemRows(
        Cluster cluster,
        SchemaElement targetType,
        String targetKeyspace,
        String targetName,
        List<String> targetSignature,
        Connection connection,
        VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException,
            InterruptedException {
      boolean isSchemaOrKeyspace = (targetType == null || targetType == KEYSPACE);

      ResultSetFuture ksFuture = null,
          udtFuture = null,
          cfFuture = null,
          colsFuture = null,
          functionsFuture = null,
          aggregatesFuture = null,
          indexesFuture = null,
          viewsFuture = null;

      ProtocolVersion protocolVersion =
          cluster.getConfiguration().getProtocolOptions().getProtocolVersion();

      if (isSchemaOrKeyspace)
        ksFuture =
            queryAsync(
                SELECT_KEYSPACES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);

      if (isSchemaOrKeyspace || targetType == TYPE)
        udtFuture =
            queryAsync(
                SELECT_USERTYPES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);

      if (isSchemaOrKeyspace || targetType == TABLE) {
        cfFuture =
            queryAsync(
                SELECT_TABLES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        colsFuture =
            queryAsync(
                SELECT_COLUMNS
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        indexesFuture =
            queryAsync(
                SELECT_INDEXES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        viewsFuture =
            queryAsync(
                SELECT_VIEWS
                    + whereClause(
                        targetType == TABLE ? VIEW : targetType,
                        targetKeyspace,
                        targetName,
                        targetSignature),
                connection,
                protocolVersion);
      }

      if (isSchemaOrKeyspace || targetType == FUNCTION)
        functionsFuture =
            queryAsync(
                SELECT_FUNCTIONS
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);

      if (isSchemaOrKeyspace || targetType == AGGREGATE)
        aggregatesFuture =
            queryAsync(
                SELECT_AGGREGATES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);

      return new SystemRows(
          get(ksFuture),
          groupByKeyspace(get(cfFuture)),
          groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, TABLE_NAME),
          groupByKeyspace(get(udtFuture)),
          groupByKeyspace(get(functionsFuture)),
          groupByKeyspace(get(aggregatesFuture)),
          groupByKeyspace(get(viewsFuture)),
          groupByKeyspaceAndCf(get(indexesFuture), TABLE_NAME),
          null,
          Collections.<String, List<Row>>emptyMap(),
          Collections.<String, Map<String, Map<String, ColumnMetadata.Raw>>>emptyMap());
    }

    @Override
    String tableNameColumn() {
      return TABLE_NAME;
    }

    protected static String whereClause(
        SchemaElement targetType,
        String targetKeyspace,
        String targetName,
        List<String> targetSignature) {
      String whereClause = "";
      if (targetType != null) {
        whereClause = " WHERE keyspace_name = '" + targetKeyspace + '\'';
        if (targetType == TABLE) whereClause += " AND table_name = '" + targetName + '\'';
        else if (targetType == VIEW) whereClause += " AND view_name = '" + targetName + '\'';
        else if (targetType == TYPE) whereClause += " AND type_name = '" + targetName + '\'';
        else if (targetType == FUNCTION)
          whereClause +=
              " AND function_name = '"
                  + targetName
                  + "' AND argument_types = "
                  + LIST_OF_TEXT_CODEC.format(targetSignature);
        else if (targetType == AGGREGATE)
          whereClause +=
              " AND aggregate_name = '"
                  + targetName
                  + "' AND argument_types = "
                  + LIST_OF_TEXT_CODEC.format(targetSignature);
      }
      return whereClause;
    }

    // Used by maybeSortUdts to sort at each dependency group alphabetically.
    private static final Comparator<Row> sortByTypeName =
        new Comparator<Row>() {
          @Override
          public int compare(Row o1, Row o2) {
            String type1 = o1.getString(UserType.TYPE_NAME);
            String type2 = o2.getString(UserType.TYPE_NAME);

            if (type1 == null && type2 == null) {
              return 0;
            } else if (type2 == null) {
              return 1;
            } else if (type1 == null) {
              return -1;
            } else {
              return type1.compareTo(type2);
            }
          }
        };

    @Override
    protected List<Row> maybeSortUdts(List<Row> udtRows, Cluster cluster, String keyspace) {
      if (udtRows.size() < 2) return udtRows;

      // For C* 3+, user-defined type resolution must be done in proper order
      // to guarantee that nested UDTs get resolved
      DirectedGraph<Row> graph = new DirectedGraph<Row>(sortByTypeName, udtRows);
      for (Row from : udtRows) {
        for (Row to : udtRows) {
          if (from != to && dependsOn(to, from, cluster, keyspace)) graph.addEdge(from, to);
        }
      }
      return graph.topologicalSort();
    }

    private boolean dependsOn(Row udt1, Row udt2, Cluster cluster, String keyspace) {
      List<String> fieldTypes = udt1.getList(UserType.COLS_TYPES, String.class);
      String typeName = udt2.getString(UserType.TYPE_NAME);
      for (String fieldTypeStr : fieldTypes) {
        // use shallow user types since some definitions might not be known at this stage
        DataType fieldType =
            DataTypeCqlNameParser.parse(fieldTypeStr, cluster, keyspace, null, null, false, true);
        if (references(fieldType, typeName)) return true;
      }
      return false;
    }

    private boolean references(DataType dataType, String typeName) {
      if (dataType instanceof UserType.Shallow
          && ((UserType.Shallow) dataType).typeName.equals(typeName)) return true;
      for (DataType arg : dataType.getTypeArguments()) {
        if (references(arg, typeName)) return true;
      }
      if (dataType instanceof TupleType) {
        for (DataType arg : ((TupleType) dataType).getComponentTypes()) {
          if (references(arg, typeName)) return true;
        }
      }
      return false;
    }
  }

  private static class V4SchemaParser extends V3SchemaParser {

    private static final String SELECT_VIRTUAL_KEYSPACES =
        "SELECT * FROM system_virtual_schema.keyspaces";
    private static final String SELECT_VIRTUAL_TABLES =
        "SELECT * FROM system_virtual_schema.tables";
    private static final String SELECT_VIRTUAL_COLUMNS =
        "SELECT * FROM system_virtual_schema.columns";

    private static final String TABLE_NAME = "table_name";

    @Override
    SystemRows fetchSystemRows(
        Cluster cluster,
        SchemaElement targetType,
        String targetKeyspace,
        String targetName,
        List<String> targetSignature,
        Connection connection,
        VersionNumber cassandraVersion)
        throws ConnectionException, BusyConnectionException, ExecutionException,
            InterruptedException {

      boolean isSchemaOrKeyspace = (targetType == null || targetType == KEYSPACE);

      ResultSetFuture ksFuture = null,
          udtFuture = null,
          cfFuture = null,
          colsFuture = null,
          functionsFuture = null,
          aggregatesFuture = null,
          indexesFuture = null,
          viewsFuture = null,
          virtualKeyspacesFuture = null,
          virtualTableFuture = null,
          virtualColumnsFuture = null;

      ProtocolVersion protocolVersion =
          cluster.getConfiguration().getProtocolOptions().getProtocolVersion();

      if (isSchemaOrKeyspace) {
        ksFuture =
            queryAsync(
                SELECT_KEYSPACES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        virtualKeyspacesFuture =
            queryAsync(
                SELECT_VIRTUAL_KEYSPACES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        virtualColumnsFuture =
            queryAsync(
                SELECT_VIRTUAL_COLUMNS
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        virtualTableFuture =
            queryAsync(
                SELECT_VIRTUAL_TABLES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
      }

      if (isSchemaOrKeyspace || targetType == TYPE) {
        udtFuture =
            queryAsync(
                SELECT_USERTYPES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
      }

      if (isSchemaOrKeyspace || targetType == TABLE) {
        cfFuture =
            queryAsync(
                SELECT_TABLES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        colsFuture =
            queryAsync(
                SELECT_COLUMNS
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        indexesFuture =
            queryAsync(
                SELECT_INDEXES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
        viewsFuture =
            queryAsync(
                SELECT_VIEWS
                    + whereClause(
                        targetType == TABLE ? VIEW : targetType,
                        targetKeyspace,
                        targetName,
                        targetSignature),
                connection,
                protocolVersion);
      }

      if (isSchemaOrKeyspace || targetType == FUNCTION) {
        functionsFuture =
            queryAsync(
                SELECT_FUNCTIONS
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
      }

      if (isSchemaOrKeyspace || targetType == AGGREGATE) {
        aggregatesFuture =
            queryAsync(
                SELECT_AGGREGATES
                    + whereClause(targetType, targetKeyspace, targetName, targetSignature),
                connection,
                protocolVersion);
      }

      return new SystemRows(
          get(ksFuture),
          groupByKeyspace(get(cfFuture)),
          groupByKeyspaceAndCf(get(colsFuture), cassandraVersion, TABLE_NAME),
          groupByKeyspace(get(udtFuture)),
          groupByKeyspace(get(functionsFuture)),
          groupByKeyspace(get(aggregatesFuture)),
          groupByKeyspace(get(viewsFuture)),
          groupByKeyspaceAndCf(get(indexesFuture), TABLE_NAME),
          get(virtualKeyspacesFuture),
          groupByKeyspace(get(virtualTableFuture)),
          groupByKeyspaceAndCf(get(virtualColumnsFuture), cassandraVersion, TABLE_NAME));
    }
  }
}
