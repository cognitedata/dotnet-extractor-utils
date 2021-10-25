using Cognite.Extensions;
using CogniteSdk;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Cognite.Extractor.Utils
{
    /// <summary>
    /// Configuration for extraction pipeline run
    /// </summary>
    public class ExtractionRunConfig
    {
        /// <summary>
        /// ExternalId of extraction pipeline.
        /// </summary>
        public string PipelineId { get; set; }
        /// <summary>
        /// Frequency of extraction pipeline updates in seconds.
        /// </summary>
        public int Frequency { get; set; } = 600;
    }

    /// <summary>
    /// Container for reporting to the ExtPipes endpoint in CDF.
    /// </summary>
    public sealed class ExtractionRun : IAsyncDisposable
    {
        private bool _finished;
        private ExtractionRunConfig _config;
        private Task _runTask;
        private CancellationTokenSource _source = new CancellationTokenSource();
        private CogniteDestination _destination;
        private ILogger<ExtractionRun> _log = new NullLogger<ExtractionRun>();

        /// <summary>
        /// True if this is a continuous extractor. This means that it should report success after the extractor is started.
        /// </summary>
        public bool Continuous { get; set; }

        /// <summary>
        /// Constructor, can be called from dependency injection if <see cref="ExtractionRunConfig"/> has been injected.
        /// </summary>
        /// <param name="config">Extraction run config object</param>
        /// <param name="destination">Cognite</param>
        /// <param name="log"></param>
        public ExtractionRun(ExtractionRunConfig config, CogniteDestination destination, ILogger<ExtractionRun> log = null)
        {
            _config = config;
            _destination = destination;
            if (log != null) _log = log;
            if (_config.PipelineId == null)
            {
                _log.LogInformation("Pipeline Id not set, extractor will not report status");
                return;
            }
        }

        /// <summary>
        /// Begin reporting, will report a success if Continuous is true.
        /// </summary>
        public void Start()
        {
            _runTask = Run();
        }
        
        private async Task Run()
        {
            IEnumerable<ExtPipe> pipe;
            try
            {
                pipe = await _destination.CogniteClient.ExtPipes.RetrieveAsync(
                    new[] { Identity.Create(_config.PipelineId) }, true, _source.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogError("Failed to fetch extraction pipeline with ExternalId: {id}, this extractor will not report status: {msg}",
                    _config.PipelineId, ex.Message);
                return;
            }
            
            if (!pipe.Any())
            {
                _log.LogError("Did not find extraction pipeline with ExternalId: {id}, this extractor will not report status",
                    _config.PipelineId);
                return;
            }

            if (Continuous)
            {
                try
                {
                    await Report(ExtPipeRunStatus.success, false, "Extractor started", _source.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException) { }
            }

            if (_config.Frequency <= 0) return;
            var delay = TimeSpan.FromSeconds(_config.Frequency);

            while (!_source.IsCancellationRequested)
            {
                var waitTask = Task.Delay(delay, _source.Token);
                try
                {
                    await Report(ExtPipeRunStatus.seen, false, null, _source.Token).ConfigureAwait(false);
                    await waitTask.ConfigureAwait(false);
                }
                catch (TaskCanceledException) {}
            }
        }

        /// <summary>
        /// Report extraction pipeline status manually.
        /// </summary>
        /// <param name="status">Status to update with</param>
        /// <param name="final">True if this should close the pipeline run and set it to finalized.
        /// If this is false, the run will continue to report "Seen"</param>
        /// <param name="message">Optional message</param>
        /// <param name="token">Optional token</param>
        public async Task Report(ExtPipeRunStatus status, bool final, string message = null, CancellationToken token = default)
        {
            if (final)
            {
                _finished = true;
                _source.Cancel();
                if (_runTask != null) await _runTask.ConfigureAwait(false);
            }
            message = message?.Truncate(1000);
            try
            {
                await _destination.CogniteClient.ExtPipes.CreateRunsAsync(new[]
                {
                    new ExtPipeRunCreate
                    {
                        ExternalId = _config.PipelineId,
                        Message = message,
                        Status = status
                    }
                }, token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log.LogWarning("Failed to report extraction pipeline status: {msg}", ex.Message);
            }
        }

        /// <summary>
        /// Dispose of the run. Will report success unless the run has already been set to finished.
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            if (_finished) return;
            await Report(ExtPipeRunStatus.success, true, "Finished without error", CancellationToken.None).ConfigureAwait(false);
        }
    }
}
