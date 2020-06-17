using Cognite.Common;
using Cognite.Extractor.Common;
using LiteDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;

namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Utilities used in state-storage
    /// </summary>
    public static class StateStoreUtils
    {
        /// <summary>
        /// Return a custom <see cref="BsonMapper"/> used to create Bson for litedb and Json for raw.
        /// </summary>
        public static BsonMapper BuildMapper()
        {
            var mapper = new BsonMapper();
            mapper.TrimWhitespace = false;
            mapper.ResolveFieldName = field => field.ToSnakeCase();
            mapper.ResolveMember += CustomResolver;
            return mapper;
        }

        private static void CustomResolver(Type type, MemberInfo memberInfo, MemberMapper member)
        {
            if (member.DataType == typeof(DateTime))
            {
                member.Deserialize = (bson, m) =>
                {
                    long ticks = bson.AsInt64;
                    return CogniteTime.FromTicks(ticks);
                };
                member.Serialize = (dt, m) =>
                {
                    var ticks = ((DateTime)dt).TicksSinceEpoch();
                    return ticks;
                };
            }
            var attr = memberInfo.GetCustomAttribute<StateStoreProperty>();
            if (attr != null)
            {
                member.FieldName = attr.Name;
            }

        }

        /// <summary>
        /// Convert PascalCase string into snake-case.
        /// </summary>
        /// <param name="str">PascalCase string to be converted</param>
        /// <returns>snake-case string</returns>
        public static string ToSnakeCase(this string str)
        {
            return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x) ? "-" + x.ToString() : x.ToString())).ToLower();
        }

        private static object BsonToDictRec(BsonValue value)
        {
            object self;
            if (value.IsArray)
            {
                var arr = value.AsArray;
                var selfArr = new object[arr.Count];
                for (int i = 0; i < arr.Count; i++)
                {
                    selfArr[i] = BsonToDictRec(arr[i]);
                }
                self = selfArr;
            }
            else if (value.IsDocument)
            {
                var doc = value.AsDocument;
                var selfDoc = new Dictionary<string, object>();
                foreach (var kvp in doc)
                {
                    selfDoc.Add(kvp.Key, BsonToDictRec(kvp.Value));
                }
                self = selfDoc;
            }
            else
            {
                self = value.RawValue;
            }
            return self;
        }
        /// <summary>
        /// Convert a BsonDocument into nested dictionaries.
        /// </summary>
        /// <param name="value">BsonDocument to convert</param>
        /// <returns>Converted document</returns>
        public static IDictionary<string, object> BsonToDict(BsonDocument value)
        {
            var result = new Dictionary<string, object>();
            foreach (var kvp in value)
            {
                result.Add(kvp.Key, BsonToDictRec(kvp.Value));
            }


            return result;
        }
        private static BsonValue DeserializeViaBsonRec(JsonElement value)
        {
            BsonValue conv = BsonValue.Null;
            switch (value.ValueKind)
            {
                case JsonValueKind.Array:
                    var bArr = new BsonArray();
                    var enumerator = value.EnumerateArray();
                    foreach (var child in enumerator)
                    {
                        bArr.Add(DeserializeViaBsonRec(child));
                    }
                    conv = bArr;
                    break;
                case JsonValueKind.False:
                    conv = new BsonValue(false);
                    break;
                case JsonValueKind.True:
                    conv = new BsonValue(true);
                    break;
                case JsonValueKind.Null:
                    conv = BsonValue.Null;
                    break;
                case JsonValueKind.Number:
                    if (value.TryGetInt64(out long lVal))
                    {
                        conv = new BsonValue(lVal);
                    }
                    else
                    {
                        conv = new BsonValue(value.GetDouble());
                    }
                    break;
                case JsonValueKind.String:
                    conv = new BsonValue(value.GetString());
                    break;
                case JsonValueKind.Object:
                    conv = new BsonDocument();
                    foreach (var child in value.EnumerateObject())
                    {
                        conv[child.Name] = DeserializeViaBsonRec(child.Value);
                    }
                    break;
            }
            return conv;
        }

        /// <summary>
        /// Convert a jsonElement dictionary into a bsonDocument, then deserialize to type <typeparamref name="T"/>
        /// </summary>
        /// <typeparam name="T">Type to deserialize to</typeparam>
        /// <param name="raw">Json data</param>
        /// <param name="mapper">Mapper to use for deserializing</param>
        /// <returns>An instance of <typeparamref name="T"/></returns>
        public static T DeserializeViaBson<T>(IDictionary<string, JsonElement> raw, BsonMapper mapper)
        {
            var doc = new BsonDocument();
            foreach (var kvp in raw)
            {
                doc[kvp.Key] = DeserializeViaBsonRec(kvp.Value);
            }
            return mapper.Deserialize<T>(doc);
        }
        /// <summary>
        /// Configure service collection to include a state store
        /// </summary>
        /// <param name="services">Servicecollection to add to</param>
        /// <param name="bannedTypes">List of state-storage types excluded from use.</param>
        public static void AddStateStore(this IServiceCollection services, params StateStoreConfig.StorageType[] bannedTypes)
        {
            services.AddSingleton<IExtractionStateStore>(provider =>
            {
                var config = provider.GetRequiredService<StateStoreConfig>();
                if (string.IsNullOrWhiteSpace(config.Location)) return null;
                if (config.Database == StateStoreConfig.StorageType.LiteDb && !bannedTypes.Contains(StateStoreConfig.StorageType.LiteDb))
                {
                    var logger = provider.GetRequiredService<ILogger<LiteDBStateStore>>();
                    return new LiteDBStateStore(config, logger);
                }
                else if (config.Database == StateStoreConfig.StorageType.Raw && !bannedTypes.Contains(StateStoreConfig.StorageType.Raw))
                {
                    var logger = provider.GetRequiredService<ILogger<RawStateStore>>();
                    var destination = provider.GetRequiredService<IRawDestination>();
                    return new RawStateStore(config, destination, logger);
                }
                else
                {
                    return null;
                }

            });
        }
    }
}
