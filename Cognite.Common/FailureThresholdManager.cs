using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Common
{
    /// <summary>
    /// Simple threshold manager, tracks failed jobs and cancels execution if budget is exhausted
    /// </summary>
    public class FailureThresholdManager<T> : IDisposable where T : IComparable
    {
        private ConcurrentDictionary<T, byte> _failedJobs = new ConcurrentDictionary<T, byte>();
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
        private double _failureBudget;
        private double FailureBudget { get { return TotalJobCount * ThresholdPercentage / 100; } }

        /// <summary>
        /// Remaining budget for failed jobs
        /// </summary>
        public long RemainingBudget { get { return (long)Math.Floor(_failureBudget - _failedJobs.Count); } }
        private readonly CancellationTokenSource _source;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="thresholdPercentage">Threshold for failed jobs, %**,*</param>
        /// <param name="totalJobCount">Total number of jobs</param>
        /// <param name="token">Cancellation token</param>
        public FailureThresholdManager(double thresholdPercentage, long totalJobCount, CancellationToken token)
        {

            ThresholdPercentage = thresholdPercentage;
            TotalJobCount = totalJobCount;
            _failureBudget = FailureBudget;
            _source = CancellationTokenSource.CreateLinkedTokenSource(token);
        }

        /// <summary>
        /// Adds job to failed items, checks if the failure budget has been exhausted
        /// </summary>
        /// <param name="job">Threshold for failed jobs, %**,*</param>
        public void Failed(T job)
        {
            _failedJobs[job] = 0;
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
            _failureBudget = FailureBudget;
            if (validate)
                _checkThreshold();
        }

        private void _checkThreshold()
        {
            if (_failedJobs.Count > _failureBudget)
            {
                _source.Cancel();
            }
        }

        /// <summary>
        /// Dispose method.
        /// </summary>
        public void Dispose()
        {
            _source.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}