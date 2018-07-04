namespace Sitecore.Support.Data.DataProviders.Sql
{
    using Sitecore.Configuration;
    using Sitecore.Data;
    using Sitecore.Data.Archiving;
    using Sitecore.Data.DataProviders.Sql;
    using Sitecore.Data.Items;
    using Sitecore.Diagnostics;
    using Sitecore.Eventing;
    using System;
    using System.Collections.Generic;

    public class SqlDataProvider : Sitecore.Data.DataProviders.Sql.SqlDataProvider
    {
        public SqlDataProvider(SqlDataApi api) : base(api)
        {
            Assert.ArgumentNotNull(api, "api");
            this.api = api;
            BulkUpdateContext.OnLeave += delegate
            {
                if (!Settings.FastQueryDescendantsDisabled && this.DescendantsShouldBeUpdated)
                {
                    Queue<IDelayedAction> descendantsDelayedActions = this.DescendantsDelayedActions;
                    if (Settings.BulkUpdateContext.DescendantsRebuildThreshold < descendantsDelayedActions.Count)
                    {
                        descendantsDelayedActions.Clear();
                        this.StartRebuildDescendants();
                        return;
                    }
                    while (descendantsDelayedActions.Count > 0)
                    {
                        IDelayedAction delayedAction = descendantsDelayedActions.Dequeue();
                        delayedAction.Proceed();
                    }
                }
            };
            this.InitializeEvents();
        }        

        protected override void Descendants_ItemCreated(ID parentId, ID itemId)
        {
            if (this.SkipDescendantsUpdate)
            {
                Queue<IDelayedAction> descendantsDelayedActions = this.DescendantsDelayedActions;
                if (BulkUpdateContext.IsActive && descendantsDelayedActions.Count <= Settings.BulkUpdateContext.DescendantsRebuildThreshold)
                {
                    descendantsDelayedActions.Enqueue(new DescendantsItemCreated(itemId, parentId, new Action<ID, ID>(this.Descendants_ItemCreated)));
                }
                return;
            }
            if (parentId.IsNull)
            {
                return;
            }
            try
            {
                Factory.GetRetryer().ExecuteNoResult(delegate
                {
                    using (DataProviderTransaction dataProviderTransaction = this.api.CreateTransaction())
                    {
                        this.api.Execute("INSERT INTO {0}Descendants{1} ({0}ID{1}, {0}Ancestor{1}, {0}Descendant{1})\r\n                         VALUES (newid(), {2}parentId{3}, {2}childId{3})", new object[]
                        {
                            "parentId",
                            parentId,
                            "childId",
                            itemId
                        });
                        this.api.Execute("INSERT INTO {0}Descendants{1} ({0}ID{1}, {0}Ancestor{1}, {0}Descendant{1})\r\n                         SELECT newid(), {0}Ancestor{1}, {2}itemId{3}\r\n                         FROM {0}Descendants{1} \r\n                         WHERE {0}Descendant{1} = {2}parentId{3}", new object[]
                        {
                            "itemId",
                            itemId,
                            "parentId",
                            parentId
                        });
                        dataProviderTransaction.Complete();
                    }
                });
            }
            catch (Exception exception)
            {
                Log.Error("Failed to update Descendants table", exception, this);
            }
            this.RebuildThread = null;
            //sitecore.support.90160.93438
            if ((base.Database.GetItem(itemId) != null) && (base.Database.GetItem(itemId).Children.Count > 0))
            {
                foreach (Item item in base.Database.GetItem(itemId).Children)
                {
                    this.Descendants_ItemCreated(itemId, item.ID);
                }
            }
            //end of sitecore.support.90160.93438
        }

        protected override void Descendants_ItemDeleted(ID itemId)
        {
            if (this.SkipDescendantsUpdate)
            {
                Queue<IDelayedAction> descendantsDelayedActions = this.DescendantsDelayedActions;
                if (BulkUpdateContext.IsActive && descendantsDelayedActions.Count <= Settings.BulkUpdateContext.DescendantsRebuildThreshold)
                {
                    descendantsDelayedActions.Enqueue(new DescendantsItemDeleted(itemId, new Action<ID>(this.Descendants_ItemDeleted)));
                }
                return;
            }
            try
            {
                Factory.GetRetryer().ExecuteNoResult(delegate
                {
                    using (DataProviderTransaction dataProviderTransaction = this.api.CreateTransaction())
                    {
                        //sitecore.support.90160.93438
                        this.Api.Execute("DELETE FROM {0}Descendants{1} WHERE {0}Descendant{1} IN (SELECT {0}Descendant{1} FROM {0}Descendants{1} WHERE {0}Ancestor{1} = {2}itemId{3})", new object[]
                        {
                            "itemId",
                            itemId
                        });
                        // end of sitecore.support.90160.93438
                        this.api.Execute("DELETE FROM {0}Descendants{1} WHERE {0}Descendant{1} = {2}itemId{3}", new object[]
                        {
                            "itemId",
                            itemId
                        });
                        dataProviderTransaction.Complete();
                    }
                });
            }
            catch (Exception exception)
            {
                Log.Error("Failed to update Descendants table", exception, this);
            }
            this.RebuildThread = null;
        }

        //sitecore.support.90160
        protected override void DoInitializeEvents()
        {
            base.DoInitializeEvents();
            EventManager.Subscribe<RestoreItemCompletedEvent>((e, c) => this.OnRestoreItemCompleted(new ID(e.ParentId), new ID(e.ItemId)));
        }

        //sitecore.support.90160
        protected virtual void OnRestoreItemCompleted(ID parentID, ID itemID)
        {
            base.DescendantsLock.AcquireReaderLock(-1);
            try
            {
                this.Descendants_ItemCreated(parentID, itemID);
            }
            catch (Exception exception)
            {
                Log.Error("Sitecore.Support.90160: " + exception.Message, this);
            }
            finally
            {
                base.DescendantsLock.ReleaseReaderLock();
            }
        }

        #region private/internal part
        private bool eventHandlersInitialized;
        private void InitializeEvents()
        {
            if (this.eventHandlersInitialized)
            {
                return;
            }
            lock (this)
            {
                if (!this.eventHandlersInitialized)
                {
                    this.DoInitializeEvents();
                    this.eventHandlersInitialized = true;
                }
            }
        }

        private readonly SqlDataApi api;

        internal class DescendantsItemDeleted : IDelayedAction
        {
            /// <summary>
            /// Gets the item identifier.
            /// </summary>
            /// <value>
            /// The item identifier.
            /// </value>
            public ID ItemId
            {
                get;
                private set;
            }

            /// <summary>
            /// Gets the action.
            /// </summary>
            /// <value>
            /// The action.
            /// </value>
            public Action<ID> Action
            {
                get;
                private set;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="T:Sitecore.Data.DataProviders.Sql.DescendantsItemDeleted" /> class.
            /// </summary>
            /// <param name="itemId">The item identifier.</param>
            /// <param name="action">The action.</param>
            public DescendantsItemDeleted(ID itemId, Action<ID> action)
            {
                this.ItemId = itemId;
                this.Action = action;
            }

            /// <summary>
            /// Proceeds the delayed action.
            /// </summary>
            public void Proceed()
            {
                this.Action(this.ItemId);
            }
        }

        internal class DescendantsItemCreated : IDelayedAction
        {
            /// <summary>
            /// Gets the item identifier.
            /// </summary>
            /// <value>
            /// The item identifier.
            /// </value>
            public ID ItemId
            {
                get;
                private set;
            }

            /// <summary>
            /// Gets the parent identifier.
            /// </summary>
            /// <value>
            /// The parent identifier.
            /// </value>
            public ID ParentId
            {
                get;
                private set;
            }

            /// <summary>
            /// Gets the action.
            /// </summary>
            /// <value>
            /// The action.
            /// </value>
            public Action<ID, ID> Action
            {
                get;
                private set;
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="T:Sitecore.Data.DataProviders.Sql.DescendantsItemCreated" /> class.
            /// </summary>
            /// <param name="itemId">The item identifier.</param>
            /// <param name="parentId">The parent identifier.</param>
            /// <param name="action">The action.</param>
            public DescendantsItemCreated(ID itemId, ID parentId, Action<ID, ID> action)
            {
                this.ItemId = itemId;
                this.ParentId = parentId;
                this.Action = action;
            }

            /// <summary>
            /// Proceeds the delayed action.
            /// </summary>
            public void Proceed()
            {
                this.Action(this.ParentId, this.ItemId);
            }
            #endregion
        }
    }
}