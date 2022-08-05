using System;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using Cognite.Extractor.Common;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Class used to manage an extractor using a Raw database.
    /// </summary>
    public class RedisHighAvailabilityManager : HighAvailabilityManager
    {
        private readonly ConnectionMultiplexer _redis;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="config">Configuration object.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="scheduler">Scheduler.</param>
        /// <param name="source">CancellationToken source.</param>
        /// <param name="interval">Optional update state interval.</param>
        /// <param name="inactivityThreshold">Optional threshold for extractor being inactive.</param>
        public RedisHighAvailabilityManager(
            HighAvailabilityConfig config, 
            ILogger<HighAvailabilityManager> logger,
            PeriodicScheduler scheduler,
            CancellationTokenSource source,
            TimeSpan? interval = null,
            TimeSpan? inactivityThreshold = null) 
            : base(config, logger, scheduler, source, interval, inactivityThreshold)
        {
            if (_config.Redis?.ConnectionString != null)
            {
                _redis = ConnectionMultiplexer.Connect(_config.Redis.ConnectionString);   
                if (!_redis.IsConnected)
                {
                    throw new RedisConnectionException(ConnectionFailureType.UnableToConnect, "Cannot reach remote data store.");
                }
            }
            else 
            {
                throw new MissingFieldException("Add a connection string to your config.");
            }
        }

        internal override async Task UploadLogToState()
        {
            try
            {
                var db = _redis.GetDatabase();
                var log = new RedisExtractorInstance(_config.Index, DateTime.UtcNow, _state.UpdatedStatus);

                await db.StringSetAsync(ToRedisKey(false), JsonSerializer.Serialize(log));
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when uploading log to state: {Message}", ex.Message);
            }
        }

        internal override async Task UpdateExtractorState()
        {
            try
            {
                var server = _redis.GetServer(_redis.GetEndPoints().First());
                var db = _redis.GetDatabase();
                var keys = server.Keys(pattern: ToRedisKey(true));

                var extractorInstances = new List<IExtractorInstance>();
                foreach (RedisKey key in keys)
                {
                    var value = await db.StringGetAsync(key);
                    if (value.HasValue)
                    {
                        var doc = JsonDocument.Parse(value.ToString());
                        var instance = JsonSerializer.Deserialize<RedisExtractorInstance>(doc);

                        if (instance != null) extractorInstances.Add(instance);
                    }
                }

                _state.CurrentState = extractorInstances;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error when uploading log to state: {Message}", ex.Message);
            }
           
        }

        private string ToRedisKey(bool pattern)
        {
            var result = ($"{_config.Redis?.TableName}.");
            if (!pattern) result += _config.Index;
            else result += "*";

            return result;
        }
    }

    internal class RedisExtractorInstance : IExtractorInstance
    {
        public int Index { get; set; }

        public DateTime TimeStamp { get; set; }

        public bool Active { get; set; }

        internal RedisExtractorInstance(int index, DateTime timeStamp, bool active)
        {
            Index = index;
            TimeStamp = timeStamp;
            Active = active;
        }
    }
}