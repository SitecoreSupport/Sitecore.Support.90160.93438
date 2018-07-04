namespace Sitecore.Support.Data.SqlServer
{
    using Sitecore.Collections;
    using Sitecore.Configuration;
    using Sitecore.Data;
    using Sitecore.Data.DataProviders;
    using Sitecore.Data.DataProviders.Sql;
    using Sitecore.Data.DataProviders.Sql.FastQuery;
    using Sitecore.Data.DataProviders.SqlServer;
    using Sitecore.Data.Items;
    using Sitecore.Data.SqlServer;
    using Sitecore.Diagnostics;
    using Sitecore.Eventing;
    using Sitecore.Globalization;
    using Sitecore.Threading;
    using Sitecore.Workflows;
    using System;
    using System.Collections;
    using System.Data;
    using System.Data.SqlClient;
    using System.IO;
    using System.Threading;

    public class SqlServerDataProvider : Sitecore.Support.Data.DataProviders.Sql.SqlDataProvider
    {
        /// <summary>
        /// The _blob set locks.
        /// </summary>
        private readonly LockSet blobSetLocks = new LockSet();

        /// <summary>
        /// The Sql command timeout.
        /// </summary>
        /// TODO: should be removed when we change blob related method to use SqlDataApi.
        private TimeSpan commandTimeout = Settings.DefaultSQLTimeout;

        /// <summary>
        /// Gets or sets the command timeout.
        /// </summary>
        /// <value>The command timeout.</value>
        protected TimeSpan CommandTimeout
        {
            get
            {
                return this.commandTimeout;
            }
            set
            {
                this.commandTimeout = value;
            }
        }

        /// <summary>
        /// Gets the size of the individual chunks making up a BLOB.
        /// </summary>
        /// <value>The size of the BLOB chunk.</value>
        public virtual int BlobChunkSize
        {
            get
            {
                return 1029120;
            }
        }

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        private string ConnectionString
        {
            get
            {
                SqlServerDataApi sqlServerDataApi = base.Api as SqlServerDataApi;
                if (sqlServerDataApi == null)
                {
                    return string.Empty;
                }
                return sqlServerDataApi.ConnectionString;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Sitecore.Data.SqlServer.SqlServerDataProvider" /> class.
        /// </summary>
        /// <param name="connectionString">
        /// The connection string.
        /// </param>
        public SqlServerDataProvider(string connectionString) : base(new SqlServerDataApi(connectionString))
        {
        }

        /// <summary>
        /// Determines whether the specified field contains a BLOB value.
        /// </summary>
        /// <param name="blobId">
        /// The BLOB id.
        /// </param>
        /// <param name="context">
        /// The context.
        /// </param>
        /// <returns>
        /// <c>true</c> if the specified item definition has children; otherwise, <c>false</c>.
        /// </returns>
        public override bool BlobStreamExists(Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(context, "context");
            string sql = " SELECT COUNT(*) FROM {0}Blobs{1} WHERE {0}BlobId{1} = {2}blobId{3}";
            using (DataProviderReader dataProviderReader = base.Api.CreateReader(sql, new object[]
            {
                "@blobId",
                blobId
            }))
            {
                if (dataProviderReader.Read())
                {
                    return base.Api.GetInt(0, dataProviderReader) > 0;
                }
            }
            return false;
        }

        /// <summary>
        /// Removes unused blob fields
        /// </summary>
        /// <param name="context">
        /// The context.
        /// </param>
        protected override void CleanupBlobs(CallContext context)
        {
            try
            {
                Factory.GetRetryer().ExecuteNoResult(delegate
                {
                    using (DataProviderTransaction dataProviderTransaction = base.Api.CreateTransaction())
                    {
                        base.Api.Execute("\r\nWITH {0}BlobFields{1} ({0}FieldId{1})\r\nAS\r\n(  SELECT\r\n    {0}SharedFields{1}.{0}ItemId{1}\r\n  FROM\r\n    {0}SharedFields{1}\r\n  WHERE\r\n    {0}SharedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}SharedFields{1}.{0}Value{1} = 1\r\n  UNION\r\n  SELECT\r\n    {0}VersionedFields{1}.{0}ItemId{1}\r\n  FROM\r\n    {0}VersionedFields{1}\r\n  WHERE\r\n    {0}VersionedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}VersionedFields{1}.{0}Value{1} = 1\r\n  UNION\r\n  SELECT\r\n    {0}UnversionedFields{1}.{0}ItemId{1}\r\n  FROM\r\n    {0}UnversionedFields{1}\r\n  WHERE\r\n    {0}UnversionedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}UnversionedFields{1}.{0}Value{1} = 1\r\n  UNION\r\n  SELECT\r\n    {0}ArchivedFields{1}.{0}ArchivalId{1}\r\n  FROM\r\n    {0}ArchivedFields{1}\r\n  WHERE\r\n    {0}ArchivedFields{1}.{0}FieldId{1} = {2}BlobID{3}\r\n    AND {0}ArchivedFields{1}.{0}Value{1} = 1\r\n),\r\n\r\n{0}ExistingBlobs{1} ({0}BlobId{1})\r\nAS\r\n(  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}SharedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}SharedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}SharedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}SharedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}SharedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}SharedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}VersionedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}VersionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}VersionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}VersionedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}VersionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}VersionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}UnversionedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}UnversionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}UnversionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}UnversionedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}UnversionedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}UnversionedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}ArchivedFields{1}\r\n      ON '{{' + CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) + '}}' = {0}ArchivedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}ArchivedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1}\r\n  UNION\r\n  SELECT\r\n    {0}Blobs{1}.{0}BlobId{1}\r\n  FROM\r\n    {0}Blobs{1}\r\n    JOIN {0}ArchivedFields{1}\r\n      ON CONVERT(NVARCHAR(MAX), {0}Blobs{1}.{0}BlobId{1}) = {0}ArchivedFields{1}.{0}Value{1}\r\n    JOIN {0}BlobFields{1}\r\n      ON {0}ArchivedFields{1}.{0}FieldId{1} = {0}BlobFields{1}.{0}FieldId{1})\r\n\r\nDELETE FROM {0}Blobs{1}\r\nWHERE NOT EXISTS\r\n(  SELECT NULL\r\n  FROM {0}ExistingBlobs{1}\r\n  WHERE {0}ExistingBlobs{1}.{0}BlobId{1} = {0}Blobs{1}.{0}BlobId{1})\r\n", new object[]
                        {
                            "BlobID",
                            TemplateFieldIDs.Blob
                        });
                        dataProviderTransaction.Complete();
                    }
                });
            }
            catch (Exception exception)
            {
                Log.Error("Failed to remove unused blobs", exception, this);
            }
        }

        /// <summary>
        /// Deletes an item.
        /// </summary>
        /// <param name="itemDefinition">
        /// The item definition.
        /// </param>
        /// <param name="context">
        /// The context.
        /// </param>
        /// <returns>
        /// The delete item.
        /// </returns>
        public override bool DeleteItem(ItemDefinition itemDefinition, CallContext context)
        {
            ID parentID = this.GetParentID(itemDefinition, context);
            Item item = context.DataManager.Database.GetItem(itemDefinition.ID);
            string sql = "DELETE FROM {0}Items{1}\r\n                  WHERE {0}ID{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}SharedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n\r\n                  DELETE FROM {0}VersionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}";
            this.DescendantsLock.AcquireReaderLock(-1);
            try
            {
                Factory.GetRetryer().ExecuteNoResult(delegate
                {
                    using (DataProviderTransaction dataProviderTransaction = this.Api.CreateTransaction())
                    {
                        this.Api.Execute(sql, new object[]
                        {
                            "itemId",
                            itemDefinition.ID
                        });
                        dataProviderTransaction.Complete();
                    }
                });
                this.Descendants_ItemDeleted(itemDefinition.ID);
            }
            finally
            {
                this.DescendantsLock.ReleaseReaderLock();
            }
            if (Settings.RemoveUnusedBlobsOnSave)
            {
                ManagedThreadPool.QueueUserWorkItem(delegate (object state)
                {
                    this.RemoveRelatedBlobs(item, context);
                });
            }
            base.RemovePrefetchDataFromCache(itemDefinition.ID);
            if (!parentID.IsNull)
            {
                base.RemovePrefetchDataFromCache(parentID);
            }
            if (itemDefinition.TemplateID == TemplateIDs.Language)
            {
                base.Languages = null;
            }
            return true;
        }

        /// <summary>
        /// Gets the BLOB stream associated with a field.
        /// </summary>
        /// <param name="blobId">
        /// The BLOB id.
        /// </param>
        /// <param name="context">
        /// The call context.
        /// </param>
        /// <returns>
        /// The stream.
        /// </returns>
        public override Stream GetBlobStream(Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(context, "context");
            long blobSize = this.GetBlobSize(blobId);
            if (blobSize < 0L)
            {
                return null;
            }
            return this.OpenBlobStream(blobId, blobSize);
        }

        /// <summary>
        /// Gets all items that are in a specific workflow state taking into account SQL Server engine specific:
        /// <para>The query executed with NoLock statement, since results would be re-tested by GetItem further on.</para>
        /// <para><see cref="F:Sitecore.FieldIDs.WorkflowState" /> is set not as param to enable SQL Filtered indexes.</para>
        /// </summary>
        /// <param name="info">The workflow info.</param>
        /// <param name="context">The context.</param>
        /// <returns>
        /// Arrray of items that are in requested workflow state.
        /// </returns>
        public override DataUri[] GetItemsInWorkflowState(WorkflowInfo info, CallContext context)
        {
            string sql = "SELECT TOP ({2}maxVersionsToLoad{3}) {0}ItemId{1}, {0}Language{1}, {0}Version{1}\r\n          FROM {0}VersionedFields{1} WITH (NOLOCK) \r\n          WHERE {0}FieldId{1}={4}" + FieldIDs.WorkflowState.ToGuid().ToString("D") + "{4} \r\n          AND {0}Value{1}= {2}workflowStateFieldValue{3}\r\n          ORDER BY {0}Updated{1} desc";
            DataUri[] result;
            using (DataProviderReader dataProviderReader = base.Api.CreateReader(sql, new object[]
            {
                "maxVersionsToLoad",
                Settings.Workbox.SingleWorkflowStateVersionLoadThreshold,
                "workflowStateFieldValue",
                info.StateID
            }))
            {
                ArrayList arrayList = new ArrayList();
                while (dataProviderReader.Read())
                {
                    ID id = base.Api.GetId(0, dataProviderReader);
                    Language language = base.Api.GetLanguage(1, dataProviderReader);
                    Sitecore.Data.Version version = base.Api.GetVersion(2, dataProviderReader);
                    arrayList.Add(new DataUri(id, language, version));
                }
                result = (arrayList.ToArray(typeof(DataUri)) as DataUri[]);
            }
            return result;
        }

        /// <summary>
        /// Sets the BLOB stream associated with a field.
        /// </summary>
        /// <param name="stream">
        /// The stream.
        /// </param>
        /// <param name="blobId">
        /// The BLOB id.
        /// </param>
        /// <param name="context">
        /// The call context.
        /// </param>
        /// <returns>
        /// The set blob stream.
        /// </returns>
        public override bool SetBlobStream(Stream stream, Guid blobId, CallContext context)
        {
            Assert.ArgumentNotNull(stream, "stream");
            Assert.ArgumentNotNull(context, "context");
            object @lock = this.blobSetLocks.GetLock(blobId);
            lock (@lock)
            {
                base.Api.Execute("DELETE FROM {0}Blobs{1} WHERE {0}BlobId{1} = {2}blobId{3}", new object[]
                {
                    "@blobId",
                    blobId
                });
                string cmdText = " INSERT INTO [Blobs]( [Id], [BlobId], [Index], [Created], [Data] ) VALUES(   NewId(), @blobId, @index, @created, @data)";
                DateTime utcNow = DateTime.UtcNow;
                int num = 0;
                if (stream.CanSeek)
                {
                    stream.Seek(0L, SeekOrigin.Begin);
                }
                int blobChunkSize = this.BlobChunkSize;
                byte[] array = new byte[blobChunkSize];
                int i = stream.Read(array, 0, blobChunkSize);
                while (i > 0)
                {
                    using (SqlConnection sqlConnection = new SqlConnection(this.ConnectionString))
                    {
                        sqlConnection.Open();
                        SqlCommand sqlCommand = new SqlCommand(cmdText, sqlConnection);
                        sqlCommand.CommandTimeout = (int)this.CommandTimeout.TotalSeconds;
                        sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                        sqlCommand.Parameters.AddWithValue("@index", num);
                        sqlCommand.Parameters.AddWithValue("@created", utcNow);
                        sqlCommand.Parameters.Add("@data", SqlDbType.Image, i).Value = array;
                        sqlCommand.ExecuteNonQuery();
                    }
                    i = stream.Read(array, 0, blobChunkSize);
                    num++;
                }
            }
            return true;
        }

        /// <summary>
        /// Gets the event queue driver.
        /// </summary>
        /// <returns>
        /// The event queue driver.
        /// </returns>
        [Obsolete("Use BaseEventQueueProvider instead.")]
        public override EventQueue GetEventQueue()
        {
            return base.Database.RemoteEvents.Queue;
        }

        /// <summary>
        /// Cleanups the cyclic dependences. The items couldn't be removed by  CleanupOrphans because always have a parent but is not participated in item tree.
        /// </summary>
        protected override void CleanupCyclicDependences()
        {
            base.Api.Execute("\r\nWITH Tree ({0}ID{1}, {0}ParentID{1})\r\nAS\r\n(\r\n  SELECT\r\n    {0}Items{1}.{0}ID{1},\r\n    {0}Items{1}.{0}ParentID{1}\r\n  FROM\r\n    {0}Items{1}\r\n  WHERE\r\n    {0}Items{1}.{0}ParentID{1} = {2}nullId{3}\r\n  UNION ALL\r\n  SELECT\r\n    {0}Items{1}.{0}ID{1},\r\n    {0}Items{1}.{0}ParentID{1}\r\n  FROM\r\n    {0}Items{1}\r\n    JOIN {0}Tree{1} {0}t{1}\r\n      ON {0}Items{1}.{0}ParentID{1} = {0}t{1}.{0}ID{1}\r\n)\r\n\r\nDELETE FROM {0}Items{1} WHERE {0}Items{1}.{0}ID{1} NOT IN (SELECT {0}t{1}.{0}ID{1} FROM {0}Tree{1} {0}t{1})\r\n", new object[]
            {
                "nullId",
                ID.Null
            });
        }

        /// <summary>
        /// Creates the SQL translator.
        /// </summary>
        /// <returns>
        /// The SQL translator.
        /// </returns>
        protected override QueryToSqlTranslator CreateSqlTranslator()
        {
            return new SqlServerQueryToSqlTranslator(base.Api);
        }

        /// <summary>
        /// Loads the child ids.
        /// </summary>
        /// <param name="condition">
        /// The condition.
        /// </param>
        /// <param name="parameters">
        /// The parameters.
        /// </param>
        /// <param name="prefetchData">
        /// The working set.
        /// </param>
        protected override void LoadChildIds(string condition, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
        {
        }

        /// <summary>
        /// Gets an SQL to load initial item definitions.
        /// </summary>
        /// <param name="sql">SQL-fragment.</param>
        /// <returns>Initial item definitions SQL.</returns>
        protected override string GetLoadInitialItemDefinitionsSql(string sql)
        {
            return "\r\nDECLARE @children TABLE (ID UNIQUEIDENTIFIER PRIMARY KEY (ID))\r\nINSERT INTO @children (ID)\r\nSELECT [ID]\r\nFROM [Items] WITH (NOLOCK)\r\nWHERE " + sql + "\r\n\r\nSELECT\r\n  [Id] AS [ItemId],\r\n  0 AS [Order],\r\n  0 AS [Version],\r\n  '' AS [Language],\r\n  [Name] AS [Name],\r\n  '' AS [Value],\r\n  [TemplateID] AS [FieldId],\r\n  [MasterID],\r\n  [ParentID],\r\n  [Created]\r\nFROM\r\n  [Items]\r\nWHERE\r\n  [Id] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ParentId] AS [ItemId],\r\n  1 AS [Order],\r\n  0 AS [Version],\r\n  '' AS [Language],\r\n  NULL AS [Name],\r\n  '' AS [Value],\r\n  NULL,\r\n  NULL,\r\n  [Id],\r\n  NULL AS [Created]\r\nFROM [Items]\r\nWHERE\r\n  [ParentId] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ItemId],\r\n  2 AS [Order],\r\n  0 AS [Version],\r\n  '' AS [Language],\r\n  NULL AS [Name],\r\n  [Value],\r\n  [FieldId],\r\n  NULL,\r\n  NULL,\r\n  NULL AS [Created]\r\nFROM [SharedFields]\r\nWHERE\r\n  [ItemId] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ItemId],\r\n  2 AS [Order],\r\n  0 AS [Version],\r\n  [Language],\r\n  NULL AS [Name],\r\n  [Value],\r\n  [FieldId],\r\n  NULL,\r\n  NULL,\r\n  NULL AS [Created]\r\nFROM [UnversionedFields]\r\nWHERE [ItemId] IN (SELECT [ID] FROM @children)\r\nUNION ALL\r\nSELECT\r\n  [ItemId],\r\n  2 AS [Order],\r\n  [Version],\r\n  [Language],\r\n  NULL AS [Name],\r\n  [Value],\r\n  [FieldId],\r\n  NULL,\r\n  NULL,\r\n  NULL AS [Created]\r\nFROM [VersionedFields]\r\nWHERE\r\n  [ItemId] IN (SELECT [ID] FROM @children)\r\nORDER BY\r\n  [ItemId],\r\n  [Order] ASC,\r\n  [Language] DESC,\r\n  [Version] DESC";
        }

        /// <summary>
        /// Returns an SQL to load child items definitions.
        /// </summary>
        /// <returns>Child items definitions SQL.</returns>
        protected override string GetLoadChildItemsDefinitionsSql()
        {
            return this.GetLoadInitialItemDefinitionsSql("{0}ParentID{1} = {2}itemId{3}");
        }

        /// <summary>
        /// Returns an SQL to load item definitions.
        /// </summary>
        /// <returns>Item definitions SQL.</returns>
        protected override string GetLoadItemDefinitionsSql()
        {
            return "\r\nSELECT\r\n  [ItemId],\r\n  [Order],\r\n  [Version],\r\n  [Language],\r\n  [Name],\r\n  [Value],\r\n  [FieldId],\r\n  [MasterID],\r\n  [ParentID],\r\n  [Created]\r\nFROM\r\n( SELECT\r\n    [Id] AS [ItemId],\r\n    0 AS [Order],\r\n    0 AS [Version],\r\n    '' AS [Language],\r\n    [Name],\r\n    '' AS [Value],\r\n    [TemplateID] AS [FieldId],\r\n    [MasterID],\r\n    [ParentID],\r\n    [Created]\r\n  FROM [Items]\r\n  UNION ALL\r\n  SELECT\r\n    [ParentId] AS [ItemId],\r\n    1 AS [Order],\r\n    0 AS [Version],\r\n    '' AS [Language],\r\n    NULL AS [Name],\r\n    '',\r\n    NULL,\r\n    NULL,\r\n    [Id],\r\n    NULL\r\n  FROM [Items]\r\n  UNION ALL\r\n  SELECT\r\n    [ItemId],\r\n    2 AS [Order],\r\n    0 AS [Version],\r\n    '' AS [Language],\r\n    NULL AS [Name],\r\n    [Value],\r\n    [FieldId],\r\n    NULL,\r\n    NULL,\r\n    NULL\r\n  FROM [SharedFields]\r\n  UNION ALL\r\n  SELECT\r\n    [ItemId],\r\n    2 AS [Order],\r\n    0 AS [Version],\r\n    [Language],\r\n    NULL AS [Name],\r\n    [Value],\r\n    [FieldId],\r\n    NULL,\r\n    NULL,\r\n    NULL\r\n  FROM [UnversionedFields]\r\n  UNION ALL\r\n  SELECT\r\n    [ItemId],\r\n    2 AS [Order],\r\n    [Version],\r\n    [Language],\r\n    NULL AS [Name],\r\n    [Value],\r\n    [FieldId],\r\n    NULL,\r\n    NULL,\r\n    NULL\r\n  FROM [VersionedFields]) AS temp\r\n  WHERE {0}ItemId{1} IN (SELECT {0}ID{1} FROM {0}Items{1} WITH (NOLOCK) WHERE {0}ID{1} = {2}itemId{3})\r\n  ORDER BY [ItemId], [Order] ASC, [Language] DESC, [Version] DESC";
        }

        /// <summary>
        /// Executes item definitions SQL.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="prefetchData"></param>
        protected override void ExecuteLoadItemDefinitionsSql(string sql, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
        {
            LanguageCollection languages = this.GetLanguages();
            PrefetchData prefetchData2 = null;
            bool flag = false;
            bool flag2 = false;
            ID right = ID.Null;
            bool flag3 = false;
            int num = 5;
            while (true)
            {
                try
                {
                    using (DataProviderReader dataProviderReader = base.Api.CreateReader(sql, parameters))
                    {
                        while (dataProviderReader.Read())
                        {
                            flag3 = true;
                            ID id = base.Api.GetId(0, dataProviderReader);
                            int @int = base.Api.GetInt(1, dataProviderReader);
                            if (@int == 0)
                            {
                                string @string = base.Api.GetString(4, dataProviderReader);
                                ID id2 = base.Api.GetId(6, dataProviderReader);
                                ID id3 = base.Api.GetId(7, dataProviderReader);
                                ID id4 = base.Api.GetId(8, dataProviderReader);
                                DateTime created = DateTime.SpecifyKind(base.Api.GetDateTime(9, dataProviderReader), DateTimeKind.Utc);
                                flag = false;
                                right = id;
                                if (prefetchData.ContainsKey(id))
                                {
                                    prefetchData2 = prefetchData[id];
                                }
                                else
                                {
                                    prefetchData2 = new PrefetchData(new ItemDefinition(id, @string, id2, id3, created), id4);
                                    prefetchData[id] = prefetchData2;
                                }
                            }
                            else if (id != right || prefetchData2 == null)
                            {
                                if (!flag2)
                                {
                                    Log.Error(string.Format("Failed to get item information, ItemID: {0}", (id.IsNull) ? "NULL" : id.ToString()), this);
                                    flag2 = true;
                                }
                            }
                            else if (@int == 1)
                            {
                                ID id5 = base.Api.GetId(8, dataProviderReader);
                                prefetchData2.AddChildId(id5);
                            }
                            else
                            {
                                if (!flag)
                                {
                                    prefetchData2.InitializeFieldLists(languages);
                                    flag = true;
                                }
                                ID id6 = base.Api.GetId(6, dataProviderReader);
                                int int2 = base.Api.GetInt(2, dataProviderReader);
                                string string2 = base.Api.GetString(3, dataProviderReader);
                                string string3 = base.Api.GetString(5, dataProviderReader);
                                prefetchData2.AddField(string2, int2, id6, string3);
                            }
                        }
                    }
                }
                catch (SqlException ex)
                {
                    if (!flag3 && ex.Number == 1205 && num > 0)
                    {
                        num--;
                        Thread.Sleep(200);
                        continue;
                    }
                    throw;
                }
                break;
            }
        }

        /// <summary>
        /// Loads the item fields.
        /// </summary>
        /// <param name="itemCondition">
        /// The item condition.
        /// </param>
        /// <param name="fieldCondition">
        /// The field condition.
        /// </param>
        /// <param name="parameters">
        /// The parameters.
        /// </param>
        /// <param name="prefetchData">
        /// The working set.
        /// </param>
        protected override void LoadItemFields(string itemCondition, string fieldCondition, object[] parameters, SafeDictionary<ID, PrefetchData> prefetchData)
        {
        }

        /// <summary>
        /// Rebuilds the descendants table.
        /// </summary>
        protected override void RebuildDescendants()
        {
            this.DescendantsLock.AcquireWriterLock(-1);
            try
            {
                Factory.GetRetryer().ExecuteNoResult(delegate
                {
                    using (DataProviderTransaction dataProviderTransaction = base.Api.CreateTransaction())
                    {
                        base.Api.Execute("\r\nDECLARE @descendants TABLE (\r\n  {0}Ancestor{1} {0}uniqueidentifier{1} NOT NULL,\r\n  {0}Descendant{1} {0}uniqueidentifier{1} NOT NULL,\r\n  PRIMARY KEY({0}Ancestor{1}, {0}Descendant{1})\r\n);\r\n\r\nWITH TempSet({0}Ancestor{1}, {0}Descendant{1}) AS\r\n(\r\n  SELECT\r\n    {0}Items{1}.{0}ParentID{1},\r\n    {0}Items{1}.{0}ID{1}\r\n  FROM\r\n    {0}Items{1}\r\n  UNION ALL\r\n  SELECT\r\n    {0}Items{1}.{0}ParentID{1},\r\n    {0}TempSet{1}.{0}Descendant{1}\r\n  FROM\r\n    {0}Items{1} JOIN {0}TempSet{1}\r\n      ON {0}TempSet{1}.{0}Ancestor{1} = {0}Items{1}.{0}ID{1}\r\n)\r\nINSERT INTO @descendants({0}Ancestor{1}, {0}Descendant{1})\r\nSELECT\r\n  {0}TempSet{1}.{0}Ancestor{1},\r\n  {0}TempSet{1}.{0}Descendant{1}\r\nFROM\r\n  {0}TempSet{1}\r\nOPTION (MAXRECURSION 32767)\r\nMERGE {0}Descendants{1} AS {0}Target{1}\r\nUSING @descendants AS {0}Source{1}\r\nON (\r\n  {0}Target{1}.{0}Ancestor{1} = {0}Source{1}.{0}Ancestor{1}\r\n  AND {0}Target{1}.{0}Descendant{1} = {0}Source{1}.{0}Descendant{1}\r\n)\r\nWHEN NOT MATCHED BY TARGET THEN\r\n  INSERT ({0}ID{1}, {0}Ancestor{1}, {0}Descendant{1})\r\n  VALUES (NEWID(), {0}Source{1}.{0}Ancestor{1}, {0}Source{1}.{0}Descendant{1})\r\nWHEN NOT MATCHED BY SOURCE THEN\r\n  DELETE;\r\n", Array.Empty<object>());
                        dataProviderTransaction.Complete();
                    }
                });
            }
            catch (Exception exception)
            {
                Log.Error("Failed to rebuild Descendants table", exception, this);
            }
            finally
            {
                base.DescendantsShouldBeUpdated = false;
                this.DescendantsLock.ReleaseWriterLock();
            }
            base.RebuildThread = null;
        }

        /// <summary>
        /// Removes the fields.
        /// </summary>
        /// <param name="itemId">
        /// The item id.
        /// </param>
        /// <param name="language">
        /// The language.
        /// </param>
        /// <param name="version">
        /// The version.
        /// </param>
        protected override void RemoveFields(ID itemId, Language language, Sitecore.Data.Version version)
        {
            string sql = "DELETE FROM {0}SharedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                \r\n                  DELETE FROM {0}UnversionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n                \r\n                  DELETE FROM {0}VersionedFields{1}\r\n                  WHERE {0}ItemId{1} = {2}itemId{3}\r\n                  AND {0}Language{1} = {2}language{3}\r\n                  AND {0}Version{1} = {2}version{3}";
            base.Api.Execute(sql, new object[]
            {
                "itemId",
                itemId,
                "language",
                language,
                "version",
                version
            });
        }

        /// <summary>
        /// Gets the SQL which checks if BLOB should be deleted.
        /// </summary>
        /// <returns>
        /// SQL which checks if BLOB should be deleted.
        /// </returns>
        protected override string GetCheckIfBlobShouldBeDeletedSql()
        {
            return "\r\nIF EXISTS (SELECT NULL FROM {0}SharedFields{1} WITH (NOLOCK) WHERE {0}SharedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND\r\nELSE IF EXISTS (SELECT NULL FROM {0}VersionedFields{1} WITH (NOLOCK) WHERE {0}VersionedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND\r\nELSE IF EXISTS (SELECT NULL FROM {0}ArchivedFields{1} WITH (NOLOCK) WHERE {0}ArchivedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND\r\nELSE IF EXISTS (SELECT NULL FROM {0}UnversionedFields{1} WITH (NOLOCK) WHERE {0}UnversionedFields{1}.{0}Value{1} LIKE {2}blobId{3})\r\nBEGIN\r\n  SELECT 1\r\nEND";
        }

        /// <summary>
        /// Gets the size of a BLOB.
        /// </summary>
        /// <param name="blobId">
        /// The BLOB id.
        /// </param>
        /// <returns>
        /// The get blob size.
        /// </returns>
        private long GetBlobSize(Guid blobId)
        {
            string cmdText = "SELECT SUM(CAST(DATALENGTH([Data]) AS BIGINT)) FROM [Blobs] WHERE [BlobId] = @blobId";
            using (SqlConnection sqlConnection = this.OpenConnection())
            {
                SqlCommand sqlCommand = new SqlCommand(cmdText, sqlConnection);
                sqlCommand.CommandTimeout = (int)this.CommandTimeout.TotalSeconds;
                sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection))
                {
                    if (sqlDataReader == null)
                    {
                        long result = -1L;
                        return result;
                    }
                    if (sqlDataReader.Read())
                    {
                        long result;
                        if (sqlDataReader.IsDBNull(0))
                        {
                            result = -1L;
                            return result;
                        }
                        result = SqlServerHelper.GetLong(sqlDataReader, 0);
                        return result;
                    }
                }
            }
            return -1L;
        }

        /// <summary>
        /// Opens the BLOB stream.
        /// </summary>
        /// <param name="blobId">
        /// The BLOB id.
        /// </param>
        /// <param name="blobSize">
        /// Size of the BLOB.
        /// </param>
        /// <returns>Stream object.
        /// </returns>
        private Stream OpenBlobStream(Guid blobId, long blobSize)
        {
            string cmdText = "SELECT [Data]\r\n                     FROM [Blobs]\r\n                     WHERE [BlobId] = @blobId\r\n                     ORDER BY [Index]";
            SqlConnection sqlConnection = this.OpenConnection();
            try
            {
                SqlCommand sqlCommand = new SqlCommand(cmdText, sqlConnection);
                sqlCommand.CommandTimeout = (int)this.CommandTimeout.TotalSeconds;
                sqlCommand.Parameters.AddWithValue("@blobId", blobId);
                SqlDataReader sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
                try
                {
                    return new SqlServerStream(sqlDataReader, blobSize);
                }
                catch (Exception exception)
                {
                    if (sqlDataReader != null)
                    {
                        sqlDataReader.Close();
                    }
                    Log.Error("Error reading blob stream (blob id: " + blobId + ")", exception, this);
                }
            }
            catch (Exception exception2)
            {
                sqlConnection.Close();
                Log.Error("Error reading blob stream (blob id: " + blobId + ")", exception2, this);
            }
            return null;
        }

        /// <summary>
        /// Opens a connection to the database.
        /// </summary>
        /// <returns>SqlConnection object.
        /// </returns>
        private SqlConnection OpenConnection()
        {
            SqlConnection sqlConnection = new SqlConnection(this.ConnectionString);
            sqlConnection.Open();
            return sqlConnection;
        }
    }
}