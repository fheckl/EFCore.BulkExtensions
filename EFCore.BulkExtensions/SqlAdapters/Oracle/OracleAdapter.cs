using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using NetTopologySuite.Geometries;
using Oracle.ManagedDataAccess.Client;

namespace EFCore.BulkExtensions.SqlAdapters.Oracle;

/// <inheritdoc />
public class OracleAdapter : ISqlOperationsAdapter
{
    
    #region Insert
    
    /// <inheritdoc />
    public void Insert<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }
    
    /// <inheritdoc />
    public async Task InsertAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, CancellationToken.None).ConfigureAwait(false);
    }
    
    /// <summary>
    /// Inserts the given data
    /// </summary>
    /// <param name="context">The database context</param>
    /// <param name="type">The type of the data to insert</param>
    /// <param name="entities">The entities to insert</param>
    /// <param name="tableInfo">The table metadata information</param>
    /// <param name="progress">The progress action</param>
    /// <param name="isAsync">Indicates if the caller requested sync or async mode</param>
    /// <param name="cancellationToken">The token to cancel a request</param>
    /// <typeparam name="T">The type parameter of the entities to insert</typeparam>
    protected static async Task InsertAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, 
        bool isAsync, CancellationToken cancellationToken)
    {
        tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities);
        if (isAsync)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.OpenConnection();
        }
        var connection = context.GetUnderlyingConnection(tableInfo.BulkConfig);
        try
        {
            var transaction = context.Database.CurrentTransaction;
            var oracleBulkCopy = GetOracleBulkCopy((OracleConnection)connection);

            SetOracleBulkCopyConfig(oracleBulkCopy, tableInfo);

            var dataTable = GetDataTable(context, type, entities, oracleBulkCopy, tableInfo);
            var dataReader = tableInfo.BulkConfig.DataReader;

            if (dataReader == null)
                oracleBulkCopy.WriteToServer(dataTable, DataRowState.Added);
            else
                oracleBulkCopy.WriteToServer(dataReader);
        }
        finally
        {
            if (isAsync)
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                context.Database.CloseConnection();
            }
        }
        if (!tableInfo.CreateOutputTable)
        {
            tableInfo.CheckToSetIdentityForPreserveOrder(tableInfo, entities, reset: true);
        }
    }
    
    private static OracleBulkCopy GetOracleBulkCopy(OracleConnection oracleConnection)
    {
        var oracleBulkCopy = new OracleBulkCopy(oracleConnection, OracleBulkCopyOptions.Default);
        
        return oracleBulkCopy;
    }
    /// <summary>
    /// Supports <see cref="OracleBulkCopy"/>
    /// </summary>
    /// <param name="oracleBulkCopy">The oracle bulk copy objects</param>
    /// <param name="tableInfo">The table metadata information</param>
    private static void SetOracleBulkCopyConfig(OracleBulkCopy oracleBulkCopy, TableInfo tableInfo)
    {
        var destinationTable = tableInfo.InsertToTempTable ? tableInfo.FullTempTableName
            : tableInfo.FullTableName;
        destinationTable = destinationTable.Replace("[", "\"")
            .Replace("]", "\"");
        oracleBulkCopy.DestinationTableName = destinationTable;
        
        oracleBulkCopy.NotifyAfter = tableInfo.BulkConfig.NotifyAfter ?? tableInfo.BulkConfig.BatchSize;
        oracleBulkCopy.BulkCopyTimeout = tableInfo.BulkConfig.BulkCopyTimeout ?? oracleBulkCopy.BulkCopyTimeout;
    }
    
    #region DataTable
    /// <summary>
    /// Supports <see cref="MySqlBulkCopy"/>
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="oracleBulkCopy"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    private static DataTable GetDataTable<T>(DbContext context, Type type, IEnumerable<T> entities, OracleBulkCopy oracleBulkCopy, TableInfo tableInfo)
    {
        var dataTable = GetDataTable(context, ref type, entities, tableInfo);

        var sourceOrdinal = 0;
        foreach (DataColumn item in dataTable.Columns)  //Add mapping
        {
            oracleBulkCopy.ColumnMappings.Add(new OracleBulkCopyColumnMapping(sourceOrdinal, item.ColumnName));
            sourceOrdinal++;
        }
        return dataTable;
    }

    /// <summary>
    /// Common logic for two versions of GetDataTable
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="context"></param>
    /// <param name="type"></param>
    /// <param name="entities"></param>
    /// <param name="tableInfo"></param>
    /// <returns></returns>
    private static DataTable GetDataTable<T>(DbContext context, ref Type type, IEnumerable<T> entities, TableInfo tableInfo)
    {
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var databaseType = SqlAdaptersMapping.GetDatabaseType();
        var isOracle = databaseType == SqlType.Oracle;
        
        var objectIdentifier = tableInfo.ObjectIdentifier;
        var entitiesList = entities.ToList();
        type = tableInfo.HasAbstractList ? entitiesList[0]!.GetType() : type;
        var entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties().ToList();

        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);

        var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned())
                                                                   .ToDictionary(a => a.Name, a => a);

        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                               .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict.ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict.Where(a => a.Value.IsShadowProperty())
                                                                .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultValueOnInsert = tableInfo.BulkConfig is { OperationType: OperationType.Insert, SetOutputIdentity: false }
                                          && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.TryGetValue(property.Name, out var propertyEntityType))
            {
                var columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType
                                                 : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isOracle && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(MySqlGeometry);
                    tableInfo.HasSpatialType = true;
                    if (tableInfo.BulkConfig.PropertiesToIncludeOnCompare is not null)
                    {
                        throw new InvalidOperationException("OnCompare properties Config can not be set for Entity with Spatial types like 'Geometry'");
                    }
                }
                if (isOracle && (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId))))
                {
                    propertyType = typeof(byte[]);
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultValueOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out var fk))
            {
                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out var entityProperty);
                if (entityProperty == null) // BulkRead
                    continue;

                var columnName = entityProperty.GetColumnName(objectIdentifier);

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType
                                                 : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) && isOracle)
                {
                    propertyType = typeof(byte[]);
                }

                if (propertyType == typeof(HierarchyId) && isOracle)
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultValueOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
            {
                //Type? navOwnedType = type.Assembly.GetType(property.PropertyType.FullName!); // was not used

                var ownedEntityType = context.Model.FindEntityType(property.PropertyType) 
                                      ?? context.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                
                var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? [];
                var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                foreach (var ownedEntityProperty in ownedEntityProperties)
                {
                    if (ownedEntityProperty.IsPrimaryKey()) continue;
                    
                    var columnName = ownedEntityProperty.GetColumnName(objectIdentifier);
                    if (columnName is null || !tableInfo.PropertyColumnNamesDict.ContainsValue(columnName)) continue;
                    
                    ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                    ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
                }

                var innerProperties = property.PropertyType.GetProperties();
                if (tableInfo.LoadOnlyPKColumn) continue;
                {
                    foreach (var innerProperty in innerProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.TryGetValue(innerProperty.Name, out var columnName))
                        {
                            var propertyName = $"{property.Name}_{innerProperty.Name}";
                            
                            if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(propertyName, out var converter))
                            {
                                var underlyingType = Nullable.GetUnderlyingType(converter.ProviderClrType) ?? converter.ProviderClrType;
                                dataTable.Columns.Add(columnName, underlyingType);
                            }
                            else
                            {
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;
                                dataTable.Columns.Add(columnName, ownedPropertyType);
                            }
                            
                            columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                        }
                    }
                }
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (var shadowProperty in entityPropertiesDict.Values.Where(a => a.IsShadowProperty()))
            {
                var columnName = shadowProperty.GetColumnName(objectIdentifier);

                // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                if (columnName is not null && dataTable.Columns.Contains(columnName))
                    continue;

                var isConvertible = columnName is not null && tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);

                var propertyType = isConvertible
                    ? tableInfo.ConvertibleColumnConverterDict[columnName!].ProviderClrType
                    : shadowProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (isOracle && (propertyType == typeof(Geometry) || propertyType.IsSubclassOf(typeof(Geometry))))
                {
                    propertyType = typeof(byte[]);
                }

                if (isOracle && (propertyType == typeof(HierarchyId) || propertyType.IsSubclassOf(typeof(HierarchyId))))
                {
                    propertyType = typeof(byte[]);
                }

                dataTable.Columns.Add(columnName, propertyType);
                columnsDict.Add(shadowProperty.Name, null);
            }
        }

        if (discriminatorColumn != null)
        {
            var discriminatorProperty = entityPropertiesDict[discriminatorColumn];

            dataTable.Columns.Add(discriminatorColumn, discriminatorProperty.ClrType);
            columnsDict.Add(discriminatorColumn, entityType.GetDiscriminatorValue());
        }
        var hasConverterProperties = tableInfo.ConvertiblePropertyColumnDict.Count > 0;

        foreach (var entity in entitiesList)
        {
            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db

            foreach (var property in propertiesToLoad)
            {
                var propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                                           && !tableInfo.BulkConfig.SetOutputIdentity
                                           && tableInfo.DefaultValueProperties.Contains(property.Name);

                if (isOracle && tableInfo.BulkConfig.DateTime2PrecisionForceRound
                            && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.TryGetValue(property.Name, out var precision))
                {
                    var dateTimePropertyValue = (DateTime?)propertyValue;

                    if (dateTimePropertyValue is not null)
                    {
                        var digitsToRemove = 7 - precision;
                        var powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        var subsecondTicks = dateTimePropertyValue.Value!.Ticks % 10000000;
                        var ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        var roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.Value!.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
                    }
                }

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.TryGetValue(property.Name, out var columnName1))
                {
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName1].ConvertToProvider.Invoke(propertyValue);
                }

                //TODO: Hamdling special types

                if (tableInfo.HasSpatialType && propertyValue is Geometry geometryValue)
                {
                    geometryValue.SRID = tableInfo.BulkConfig.SRID;
                    var wkb = geometryValue.ToBinary();
                    propertyValue = MySqlGeometry.FromWkb(geometryValue.SRID, wkb);
                }

                if (propertyValue is HierarchyId hierarchyValue && isOracle)
                {
                    using MemoryStream memStream = new();
                    using BinaryWriter binWriter = new(memStream);

                    //hierarchyValue.Write(binWriter); // removed as of EF8 (throws: Error CS1061  'HierarchyId' does not contain a definition for 'Write' and no accessible extension method 'Write' accepting a first argument of type 'HierarchyId' could be found.
                    propertyValue = memStream.ToArray();
                }
                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out var foreignKeyShadowProperty))
                {
                    var columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    };
                    columnsDict[columnName] = propertyValue != null ? foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue) // TODO Try to optimize
                                                                    : propertyValue;
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (var ownedProperty in ownedProperties)
                    {
                        var columnName = $"{property.Name}_{ownedProperty.Name}";
                        var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out var converter))
                        {
                            columnsDict[columnName] = ownedPropertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
                        }
                        else
                        {
                            columnsDict[columnName] = ownedPropertyValue;
                        }
                    }
                }
            }

            if (tableInfo.BulkConfig.EnableShadowProperties)
            {
                foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    var shadowProperty = entityPropertiesDict[shadowPropertyName];
                    var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    var propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = context.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                    {
                        propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            var record = columnsDict.Values.ToArray();
            dataTable.Rows.Add(record);
        }

        return dataTable;
    }

    private static string? GetDiscriminatorColumn(TableInfo tableInfo)
    {
        string? discriminatorColumn = null;
        if (tableInfo.BulkConfig.EnableShadowProperties || tableInfo.ShadowProperties.Count <= 0)
            return discriminatorColumn;
        
        var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
        discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
        return discriminatorColumn;
    }
    #endregion
    
    #endregion
    
    /// <inheritdoc />
    public void Merge<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }
    
    /// <inheritdoc />
    public Task MergeAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType,
        Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }
    
    /// <inheritdoc />
    public void Read<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        throw new NotImplementedException();
    }
    
    /// <inheritdoc />
    public Task ReadAsync<T>(DbContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress,
        CancellationToken cancellationToken) where T : class
    {
        throw new NotImplementedException();
    }
    
    /// <inheritdoc />
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        throw new NotImplementedException();
    }
    
    /// <inheritdoc />
    public Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
