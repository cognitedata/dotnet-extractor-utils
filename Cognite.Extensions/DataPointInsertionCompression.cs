using System;
using System.Linq;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using CogniteSdk;
using Com.Cognite.V1.Timeseries.Proto;

namespace Cognite.Extensions
{
    internal static class DataPointInsertionCompression
    {
        internal static bool ShouldUseGzip(DataPointInsertionRequest request, int gzipCountLimit, out int datapointCount)
        {
            if (request == null) throw new ArgumentNullException(nameof(request));

            datapointCount = request.Items.Sum(r =>
                (r.NumericDatapoints?.Datapoints?.Count ?? 0)
                + (r.StringDatapoints?.Datapoints?.Count ?? 0)
                + (r.StateDatapoints?.Datapoints?.Count ?? 0));

            return gzipCountLimit >= 0 && datapointCount >= gzipCountLimit;
        }

        internal static Task CreateDataPointsAsync(Client client, DataPointInsertionRequest request, bool useGzip, CancellationToken token)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            if (request == null) throw new ArgumentNullException(nameof(request));

            // State datapoints are only supported through the beta data points API.
            bool hasStateDatapoints = request.Items.Any(r => r.DatapointTypeCase == DataPointInsertionItem.DatapointTypeOneofCase.StateDatapoints);

            if (useGzip)
            {
                if (hasStateDatapoints)
                {
                    return client.Beta.DataPoints.CreateAsync(request, CompressionLevel.Fastest, token);
                }
                return client.DataPoints.CreateAsync(request, CompressionLevel.Fastest, token);
            }

            if (hasStateDatapoints)
            {
                return client.Beta.DataPoints.CreateAsync(request, token);
            }
            return client.DataPoints.CreateAsync(request, token);
        }
    }
}
