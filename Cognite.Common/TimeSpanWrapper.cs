using NCrontab;
using System;
using System.Text.RegularExpressions;
using System.Threading;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Utility for containing a string converted to timespan in config objects.
    /// To use:
    /// public string MyIntervalValue { get; } = new TimeSpanWrapper(false, "s", "0");
    /// public string MyInterval { get => MyIntervalValue.RawValue; set => MyIntervalValue.RawValue = value; }
    /// 
    /// Access MyIntervalValue.Value in your code, and MyInterval is the config option.
    /// </summary>
    public class TimeSpanWrapper : ITimeSpanProvider
    {
        private static readonly Regex isNegative = new Regex("^-[0-9]+");

        private readonly bool allowZero;
        private readonly string defaultUnit;
        private readonly TimeSpan defaultValue;

        /// <summary>
        /// Whether the interval is dynamic or not, e.g. cron expression
        /// </summary>
        public virtual bool IsDynamic => false;
        /// <summary>
        /// Converted value as TimeSpan.
        /// </summary>
        public virtual TimeSpan Value { get; private set; }
        /// <summary>
        /// Internal raw value.
        /// </summary>
        protected string IntRawValue { get; set; }
        /// <summary>
        /// Raw string value of option.
        /// </summary>
        public virtual string RawValue
        {
            get => IntRawValue; set
            {
                IntRawValue = value;
                if (string.IsNullOrWhiteSpace(value))
                {
                    Value = defaultValue;
                    return;
                }
                var conv = CogniteTime.ParseTimeSpanString(value, defaultUnit);
                if (conv == null)
                {
                    if (isNegative.IsMatch(value)) conv = Timeout.InfiniteTimeSpan;
                    else throw new ArgumentException($"Invalid timespan string: {value}");
                }
                if (conv == TimeSpan.Zero && !allowZero) Value = Timeout.InfiniteTimeSpan;
                else Value = conv.Value;
            }
        }
        /// <summary>
        /// Create a new timespan wrapper. The wrapper is intended to be a singleton, i.e.
        /// it is not recreated if the config is modified.
        /// </summary>
        /// <param name="allowZero"></param>
        /// <param name="defaultUnit"></param>
        /// <param name="defaultValue"></param>
        public TimeSpanWrapper(bool allowZero, string defaultUnit, string defaultValue)
        {
            this.allowZero = allowZero;
            this.defaultUnit = defaultUnit;
            RawValue = defaultValue;
            IntRawValue = defaultValue;
            this.defaultValue = Value;
        }
    }

    /// <summary>
    /// Utility for containing a string converted to timespan in config objects.
    /// Also supports cron expresions.
    /// 
    /// To use:
    /// public string MyIntervalValue { get; } = new CronTimeSpanWrapper(false, "s", "0");
    /// public string MyInterval { get => MyIntervalValue.RawValue; set => MyIntervalValue.RawValue = value; }
    /// 
    /// Access MyIntervalValue.Value in your code, and MyInterval is the config option.
    /// </summary>
    public class CronTimeSpanWrapper : TimeSpanWrapper
    {
        private readonly bool _includeSeconds;

        private CrontabSchedule? _expression;

        /// <summary>
        /// Whether the interval is dynamic or not, e.g. cron expression
        /// </summary>
        public override bool IsDynamic => _expression != null;

        /// <summary>
        /// Create a new cron expression timespan wrapper. The wrapper is intended to be a singleton, i.e.
        /// it is not recreated if the config is modified.
        /// </summary>
        /// <param name="includeSeconds"></param>
        /// <param name="allowZero"></param>
        /// <param name="defaultUnit"></param>
        /// <param name="defaultValue"></param>
        public CronTimeSpanWrapper(bool includeSeconds, bool allowZero, string defaultUnit, string defaultValue)
            : base(allowZero, defaultUnit, defaultValue)
        {
            _includeSeconds = includeSeconds;
        }

        /// <summary>
        /// If the raw value is a cron expression this is the time to the next occurence.
        /// If not, it is the converted time span as for <see cref="TimeSpanWrapper"/>
        /// </summary>
        public override TimeSpan Value
        {
            get
            {
                if (_expression == null) return base.Value;

                return _expression.GetNextOccurrence(DateTime.UtcNow.AddSeconds(0.5)) - DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Raw string value of option.
        /// </summary>
        public override string RawValue {
            get => IntRawValue;
            set
            {

                if (!string.IsNullOrWhiteSpace(value) && (value.StartsWith("@") || value.Trim().Contains(" ")))
                {
                    IntRawValue = value;
                    _expression = CrontabSchedule.Parse(value, new CrontabSchedule.ParseOptions { IncludingSeconds = _includeSeconds });
                }
                else
                {
                    base.RawValue = value!;
                    _expression = null;
                }
            }
        }
    }
}
