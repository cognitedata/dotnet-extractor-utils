using CogniteSdk;
using System;
using System.Threading.Tasks;
using System.Threading;
using CogniteSdk.Resources;
using System.Linq;

namespace Cognite.Extensions
{
    /// <summary>
    /// Config for setting a dataset.
    /// </summary>
    public class DataSetConfig
    {
        /// <summary>
        /// Dataset external ID.
        /// </summary>
        public string? ExternalId { get; set; }
        /// <summary>
        /// Dataset ID.
        /// </summary>
        public long? Id { get; set; }
    }

    /// <summary>
    /// Extensions for cognite data sets.
    /// </summary>
    public static class DataSetExtensions
    {
        /// <summary>
        /// Retrieve the configured data set ID. This will only make a request to CDF
        /// if ExternalId is configured without Id.
        /// 
        /// Will throw an exception if configured using ExternalId and retrieval failed.
        /// </summary>
        /// <param name="resource">Client to use for retrieval.</param>
        /// <param name="config">Config to retrieve data set for.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Dataset ID if configured</returns>
        public static async Task<long?> GetId(this DataSetsResource resource, DataSetConfig config, CancellationToken token = default)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (config.Id != null) return config.Id.Value;

            var dataset = await resource.Get(config, token).ConfigureAwait(false);
            return dataset?.Id;
        }

        /// <summary>
        /// Retrieve the configured dataset. Will throw an exception if the dataset does not exist,
        /// or if retrieval failed.
        /// </summary>
        /// <param name="resource">Client to use for retrieval.</param>
        /// <param name="config">Config to retrieve data set for.</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Dataset if configured</returns>
        public static async Task<DataSet?> Get(this DataSetsResource resource, DataSetConfig config, CancellationToken token = default)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (resource == null) throw new ArgumentNullException(nameof(resource));
            Identity id;
            if (config.Id != null)
            {
                id = Identity.Create(config.Id.Value);
            }
            else if (config.ExternalId != null)
            {
                id = Identity.Create(config.ExternalId);
            }
            else
            {
                return null;
            }
            var dss = await resource.RetrieveAsync(new[] { id }, false, token).ConfigureAwait(false);
            return dss.FirstOrDefault();
        }
    }
}
