using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Simple threshold manager, tracks failed jobs and executes callback if budget is exhausted
    /// </summary>
    public class FailureThresholdManager<T, T2> where T : IComparable
    {
        private ConcurrentDictionary<T, T2> _failedJobs = new ConcurrentDictionary<T, T2>();

        /// <summary>
        /// Get failed jobs
        /// </summary>
        public IReadOnlyDictionary<T, T2> FailedJobs => new ReadOnlyDictionary<T, T2>(_failedJobs);

        private double _thresholdPercentage;

        /// <summary>
        /// Threshold for failed jobs, %**,*
        /// </summary>
        public double ThresholdPercentage
        {
            get { return _thresholdPercentage; }
            private set
            {
                if (_thresholdPercentage > 100 || _thresholdPercentage < 0)
                {
                    throw new ArgumentOutOfRangeException();
                }
                else
                {
                    _thresholdPercentage = value;
                }
            }
        }

        /// <summary>
        /// Total number of jobs
        /// </summary>
        public long TotalJobCount { get; private set; }

        private double FailureBudget => TotalJobCount * ThresholdPercentage / 100;

        /// <summary>
        /// Remaining budget for failed jobs
        /// </summary>
        public long RemainingBudget { get { return (long)Math.Floor(FailureBudget - _failedJobs.Count); } }
        private readonly Action<IReadOnlyDictionary<T, T2>> _callback;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="thresholdPercentage">Threshold for failed jobs, %**,*</param>
        /// <param name="totalJobCount">Total number of jobs</param>
        /// <param name="callback">Callback method for when the threshold is exceeded</param>
        public FailureThresholdManager(double thresholdPercentage, long totalJobCount, Action<IReadOnlyDictionary<T, T2>> callback)
        {

            ThresholdPercentage = thresholdPercentage;
            TotalJobCount = totalJobCount;
            _callback = callback;
        }

        /// <summary>
        /// Adds job to failed items, checks if the failure budget has been exhausted
        /// </summary>
        /// <param name="job">Job Id</param>
        /// <param name="value">Custom value for the failed job</param>
        public void Failed(T job, T2 value)
        {
            _failedJobs[job] = value;
            _checkThreshold();
        }

        /// <summary>
        /// Updates parameters for calculating the failure budget
        /// </summary>
        /// <param name="thresholdPercentage">Threshold for failed jobs, %**,*</param>
        /// <param name="totalJobCount">Total number of jobs</param>
        /// <param name="validate">Check if the updated failure budget is already exhausted</param>
        public void UpdateBudget(double thresholdPercentage, long totalJobCount, bool validate = true)
        {
            ThresholdPercentage = thresholdPercentage;
            TotalJobCount = totalJobCount;
            if (validate)
                _checkThreshold();
        }

        private void _checkThreshold()
        {
            if (_failedJobs.Count > FailureBudget)
            {
                _callback(FailedJobs);
            }
        }
    }
}