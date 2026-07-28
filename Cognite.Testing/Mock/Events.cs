using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.Common;
using CogniteSdk;
using Moq;
using Xunit;

namespace Cognite.Extractor.Testing.Mock
{
    /// <summary>
    /// Mock implementation of the Events API.
    /// </summary>
    public class EventsMock
    {
        private long _nextId = 10000;
        private readonly Dictionary<string, Event> _eventsByExternalId = new Dictionary<string, Event>();
        private readonly Dictionary<long, Event> _eventsById = new Dictionary<long, Event>();

        /// <summary>
        /// All mocked events.
        /// </summary>
        public ICollection<Event> Events => _eventsById.Values;


        /// <summary>
        /// Mock an event, assigning it an ID.
        /// </summary>
        /// <param name="ev">Event to add.</param>
        public void MockEvent(Event ev)
        {
            if (ev == null) throw new ArgumentNullException(nameof(ev));
            ev.Id = _nextId++;
            _eventsById[ev.Id] = ev;
            if (!string.IsNullOrEmpty(ev.ExternalId))
            {
                _eventsByExternalId[ev.ExternalId] = ev;
            }
        }

        /// <summary>
        /// Remove a mocked event by its external ID, if it is present.
        /// </summary>
        /// <param name="externalId">External ID of event to remove.</param>
        public bool Remove(string externalId)
        {
            if (_eventsByExternalId.TryGetValue(externalId, out var ev))
            {
                _eventsByExternalId.Remove(externalId);
                _eventsById.Remove(ev.Id);
                return true;
            }
            return false;
        }

        /// <summary>
        /// Mock an event with the given external ID, assigning it an ID.
        /// </summary>
        /// <param name="externalId">External ID of the event to add.</param>
        public void MockEvent(string externalId)
        {
            MockEvent(new Event
            {
                ExternalId = externalId,
                Type = "someType",
                StartTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                EndTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                CreatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                LastUpdatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
            });
        }

        /// <summary>
        /// Get an event by its identity, if it exists.
        /// </summary>
        /// <param name="id">Event ID</param>
        /// <returns>The event, if it exists.</returns>
        public Event? GetEvent(Identity id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            if (id.Id.HasValue && _eventsById.TryGetValue(id.Id.Value, out var ev))
            {
                return ev;
            }
            else if (!string.IsNullOrEmpty(id.ExternalId) && _eventsByExternalId.TryGetValue(id.ExternalId, out var ev2))
            {
                return ev2;
            }

            return null;
        }
        /// <summary>
        /// Get an event by its external ID, if it exists.
        /// </summary>
        /// <param name="externalId">Event external ID</param>
        /// <returns>The event, if it exists.</returns>

        public Event? GetEvent(string externalId)
        {
            if (externalId == null) throw new ArgumentNullException(nameof(externalId));

            if (_eventsByExternalId.TryGetValue(externalId, out var ev))
            {
                return ev;
            }

            return null;
        }

        /// <summary>
        /// Get an event by its internal ID, if it exists.
        /// </summary>
        /// <param name="id">Event ID</param>
        /// <returns>The event, if it exists.</returns>
        public Event? GetEvent(long id)
        {
            if (_eventsById.TryGetValue(id, out var ev))
            {
                return ev;
            }

            return null;
        }


        /// <summary>
        /// Clear the events mock, removing all mocked events.
        /// </summary>
        public void Clear()
        {
            _nextId = 10000;
            _eventsByExternalId.Clear();
            _eventsById.Clear();
        }

        /// <summary>
        /// Get a matcher for the /events/byids endpoint.
        /// </summary>
        /// <param name="times">Expected number of executions.</param>
        public RequestMatcher MakeGetByIdsMatcher(Times times)
        {
            return new SimpleMatcher("POST", "/events/byids", EventsByIdsImpl, times);
        }

        /// <summary>
        /// Get a matcher for the /events endpoint for creating events.
        /// </summary>
        /// <param name="times">Expected number of executions.</param>
        public RequestMatcher MakeCreateEventsMatcher(Times times)
        {
            return new SimpleMatcher("POST", "/events$", EventsCreateImpl, times);
        }

        private async Task<HttpResponseMessage> EventsCreateImpl(RequestContext context, CancellationToken token)
        {
            var events = await context.ReadJsonBody<ItemsWithoutCursor<EventCreate>>().ConfigureAwait(false);
            Assert.NotNull(events);
            var created = new List<Event>();
            var conflict = new List<string>();

            foreach (var ev in events.Items)
            {
                if (ev.ExternalId != null && _eventsByExternalId.ContainsKey(ev.ExternalId))
                {
                    conflict.Add(ev.ExternalId);
                    continue;
                }

                var newEvent = new Event
                {
                    Id = _nextId++,
                    ExternalId = ev.ExternalId,
                    Type = ev.Type,
                    StartTime = ev.StartTime,
                    EndTime = ev.EndTime,
                    Source = ev.Source,
                    Description = ev.Description,
                    CreatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                    LastUpdatedTime = DateTime.UtcNow.ToUnixTimeMilliseconds(),
                    Metadata = ev.Metadata,
                };

                _eventsById[newEvent.Id] = newEvent;
                if (newEvent.ExternalId != null)
                {
                    _eventsByExternalId[newEvent.ExternalId] = newEvent;
                }
                created.Add(newEvent);
            }

            if (conflict.Count > 0)
            {
                return context.CreateError(new CogniteError
                {
                    Code = 409,
                    Message = "Conflict",
                    Duplicated = conflict.Distinct().Select(id => MockUtils.ToMultiValueDict(new Identity(id))).ToList()
                });
            }

            return context.CreateJsonResponse(new ItemsWithoutCursor<Event> { Items = created });
        }

        private async Task<HttpResponseMessage> EventsByIdsImpl(RequestContext context, CancellationToken token)
        {
            var ids = await context.ReadJsonBody<ItemsWithIgnoreUnknownIds<RawIdentity>>().ConfigureAwait(false);
            Assert.NotNull(ids);
            var found = new List<Event>();
            var missing = new List<Identity>();

            foreach (var id in ids.Items)
            {
                Event? ev = null;
                if (id.Id.HasValue)
                {
                    _eventsById.TryGetValue(id.Id.Value, out ev);
                }
                else if (!string.IsNullOrEmpty(id.ExternalId))
                {
                    _eventsByExternalId.TryGetValue(id.ExternalId, out ev);
                }

                if (ev != null)
                {
                    found.Add(ev);
                }
                else
                {
                    missing.Add(id.ToIdentity());
                }
            }

            if (!ids.IgnoreUnknownIds && missing.Count > 0)
            {
                return context.CreateError(new CogniteError
                {
                    Code = 400,
                    Message = "Events not found",
                    Missing = missing.Distinct().Select(MockUtils.ToMultiValueDict).ToList(),
                });
            }

            return context.CreateJsonResponse(new ItemsWithoutCursor<Event>
            {
                Items = found
            });
        }
    }
}