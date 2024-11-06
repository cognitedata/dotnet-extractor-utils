using System;
using System.Collections.Generic;
using CogniteSdk;
using CogniteSdk.DataModels;
using static Cognite.Extensions.Sanitation;

namespace Cognite.Extensions.DataModels
{
    /// <summary>
    /// Collection of methods for cleaning and sanitizing objects used in
    /// requests to CDM Instances
    /// </summary>
    public static partial class DataModelSanitation
    {
        /// <summary>
        /// Sanitize a T object so that it can be safely sent to CDF.
        /// Requests may still fail due to conflicts or missing ids.
        /// </summary>
        /// <param name="instance">Instance to sanitize</param>
        public static void Sanitize<T>(this SourcedNodeWrite<T> instance)
        {
            if (instance == null) throw new ArgumentNullException(nameof(instance));
            if (instance.Space == null) throw new ArgumentNullException(nameof(instance.Space));
            if (instance.ExternalId == null) throw new ArgumentNullException(nameof(instance.ExternalId));
            instance.Space = instance.Space.Truncate(SpaceIdMax);
            instance.ExternalId = instance.ExternalId.TruncateBytes(ExternalIdMaxBytes)!;
        }

        /// <summary>
        /// Check that given T satisfies CDF limits.
        /// </summary>
        /// <param name="instance">Instance to check</param>
        /// <returns>True if instance satisfies limits</returns>
        public static ResourceType? Verify<T>(this SourcedNodeWrite<T> instance)
        {
            if (instance == null) throw new ArgumentNullException(nameof(instance));
            if (instance.Space == null) throw new ArgumentNullException(nameof(instance.Space));
            if (instance.ExternalId == null) throw new ArgumentNullException(nameof(instance.ExternalId));
            if (!instance.Space.CheckLength(SpaceIdMax)) return ResourceType.SpaceId;
            if (!instance.ExternalId.CheckLength(ExternalIdMax)) return ResourceType.ExternalId;
            if (!$"{instance.Space}{instance.ExternalId}".CheckLength(ExternalIdMaxBytes)) return ResourceType.InstanceId;
            return null;
        }

        /// <summary>
        /// Clean list of T objects, sanitizing each and removing any duplicates.
        /// The first encountered duplicate is kept.
        /// </summary>
        /// <param name="instances">T request to clean</param>
        /// <param name="mode">The type of sanitation to apply</param>
        /// <returns>Cleaned create request and optional errors for duplicated ids and legacyNames</returns>
        public static (IEnumerable<SourcedNodeWrite<T>>, IEnumerable<CogniteError<SourcedNodeWrite<T>>>) CleanInstanceRequest<T>(
            IEnumerable<SourcedNodeWrite<T>> instances,
            SanitationMode mode)
        {
            DistinctResource<SourcedNodeWrite<T>>[] instanceDistinct = new[]
            {
                new DistinctResource<SourcedNodeWrite<T>>("Duplicated instance ids", ResourceType.InstanceId,
                    ts => ts.ExternalId != null && ts.Space != null ? Identity.Create(new InstanceIdentifier(ts.Space, ts.ExternalId)) : null),
            };
            return CleanRequest(instanceDistinct, instances, Verify, Sanitize, mode);
        }
    }
}
