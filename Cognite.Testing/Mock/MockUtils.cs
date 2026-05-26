using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CogniteSdk;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// Common utilities for mock implementations of CDF APIs.
    /// </summary>
    public class MockUtils
    {
        /// <summary>
        /// Convert an Identity to a dictionary used in error responses.
        /// </summary>
        /// <param name="id">Identity to convert</param>
        /// <returns>Multivalue dictionary</returns>
        public static Dictionary<string, MultiValue> ToMultiValueDict(Identity id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            var dict = new Dictionary<string, MultiValue>();
            if (id.Id.HasValue)
            {
                dict["id"] = MultiValue.Create(id.Id.Value);
            }
            else if (id.InstanceId != null)
            {
                dict["instanceId"] = MultiValue.Create(id.InstanceId);
            }
            else if (!string.IsNullOrEmpty(id.ExternalId))
            {
                dict["externalId"] = MultiValue.Create(id.ExternalId);
            }
            return dict;
        }
    }
}