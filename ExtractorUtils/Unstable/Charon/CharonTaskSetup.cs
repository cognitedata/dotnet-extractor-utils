using System;
using System.Collections.Generic;

namespace Cognite.Extractor.Utils.Unstable.Charon
{
    /// <summary>
    /// Destination path constants for Charon tasks (POC scope: timeseries only).
    /// </summary>
    public static class CharonDestinations
    {
        /// <summary>Create timeseries.</summary>
        public const string TimeSeriesCreate = "/timeseries/create";
        /// <summary>Update timeseries.</summary>
        public const string TimeSeriesUpdate = "/timeseries/update";
        /// <summary>Insert datapoints.</summary>
        public const string TimeSeriesDatapoints = "/timeseries/datapoints";
    }

    /// <summary>
    /// Typed builders for Charon task registrations. Prefer these over hand-building
    /// <see cref="CharonSetupItem"/>.
    /// </summary>
    public static class CharonTask
    {
        /// <summary>
        /// Build a <c>space_routing</c> task. Items sent later for this task must already be fully
        /// shaped data-modeling bodies without a <c>space</c>; Charon injects the resolved space and
        /// forwards verbatim.
        ///
        /// Mapping is pattern-to-space and <b>order-significant</b>: Charon matches each item's
        /// top-level <c>externalId</c> against patterns in order, first match wins. Note that the
        /// exact regex anchoring (full-match vs substring) is not yet frozen on the Charon side.
        /// </summary>
        /// <param name="taskName">Unique task name.</param>
        /// <param name="destination">One of the <see cref="CharonDestinations"/> values.</param>
        /// <param name="mapping">Ordered pattern-to-space pairs; order determines match precedence.</param>
        /// <param name="defaultSpace">Default space when no pattern matches (required, non-empty).</param>
        /// <returns>A setup item ready to register.</returns>
        public static CharonSetupItem SpaceRouting(
            string taskName,
            string destination,
            IEnumerable<KeyValuePair<string, string>> mapping,
            string defaultSpace)
        {
            if (string.IsNullOrEmpty(taskName)) throw new ArgumentException("Task name is required", nameof(taskName));
            if (string.IsNullOrEmpty(destination)) throw new ArgumentException("Destination is required", nameof(destination));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));
            if (string.IsNullOrEmpty(defaultSpace)) throw new ArgumentException("Default space is required", nameof(defaultSpace));
            return new CharonSetupItem
            {
                TaskName = taskName,
                Type = "space_routing",
                Destination = destination,
                Mapping = new OrderedStringMap(mapping),
                Default = defaultSpace,
            };
        }

        /// <summary>
        /// Build a <c>custom</c> task. Items sent later are raw source-shaped items; Charon evaluates
        /// the mapping's Kuiper expressions per item (with the item bound as <c>input</c>) to build
        /// each destination field. Classic vs data-modeling is decided by whether the mapping
        /// contains a <c>space</c> key.
        /// </summary>
        /// <param name="taskName">Unique task name.</param>
        /// <param name="destination">One of the <see cref="CharonDestinations"/> values.</param>
        /// <param name="mapping">Destination-key to Kuiper expression pairs.</param>
        /// <returns>A setup item ready to register.</returns>
        public static CharonSetupItem Custom(
            string taskName,
            string destination,
            IEnumerable<KeyValuePair<string, string>> mapping)
        {
            if (string.IsNullOrEmpty(taskName)) throw new ArgumentException("Task name is required", nameof(taskName));
            if (string.IsNullOrEmpty(destination)) throw new ArgumentException("Destination is required", nameof(destination));
            if (mapping == null) throw new ArgumentNullException(nameof(mapping));
            return new CharonSetupItem
            {
                TaskName = taskName,
                Type = "custom",
                Destination = destination,
                Mapping = new OrderedStringMap(mapping),
            };
        }
    }
}
