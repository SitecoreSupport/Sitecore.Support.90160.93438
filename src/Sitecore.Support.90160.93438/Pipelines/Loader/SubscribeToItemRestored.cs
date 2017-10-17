namespace Sitecore.Support.Pipelines.Loader
{
    using Sitecore.Data;
    using Sitecore.Data.Archiving;
    using Sitecore.Data.DataProviders;
    using Sitecore.Diagnostics;
    using Sitecore.Eventing;
    using Sitecore.Pipelines;

    public class SubscribeToItemRestored
    {
        public void Process(PipelineArgs args)
        {
            EventManager.Subscribe<RestoreItemCompletedEvent>(delegate (RestoreItemCompletedEvent restoreItemCompletedEvent)
            {
                Database eventDatabase = Database.GetDatabase(restoreItemCompletedEvent.DatabaseName);
                DataProvider[] providers = eventDatabase.GetDataProviders();

                for (int i = 0; i < providers.Length; i++)
                {
                    DataProvider dataProvider = providers[i];
                    if (dataProvider.IsEnabled("CreateItem"))
                    {
                        Sitecore.Support.Data.SqlServer.SqlServerDataProvider supportDataProvider = dataProvider as Sitecore.Support.Data.SqlServer.SqlServerDataProvider;
                        if (supportDataProvider != null)
                            supportDataProvider.Restore(new ID(restoreItemCompletedEvent.ParentId), new ID(restoreItemCompletedEvent.ItemId));
                    }
                }
            });
        }
    }
}