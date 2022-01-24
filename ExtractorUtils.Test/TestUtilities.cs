using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Cognite.Extractor.StateStorage;
using Moq;
using Moq.Protected;
using Xunit;

namespace ExtractorUtils.Test {
    public static class TestUtilities
    {
        public static (Mock<IHttpClientFactory> factory, Mock<HttpMessageHandler> handler) GetMockedHttpClientFactory(
            Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> mockSendAsync)
        {
            var mockHttpMessageHandler = new Mock<HttpMessageHandler>();
            mockHttpMessageHandler.Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", 
                                                  ItExpr.IsAny<HttpRequestMessage>(), 
                                                  ItExpr.IsAny<CancellationToken>())
                .Returns<HttpRequestMessage, CancellationToken>(mockSendAsync);
            var client = new HttpClient(mockHttpMessageHandler.Object);
            var mockFactory = new Mock<IHttpClientFactory>();
            mockFactory.Setup(_ => _.CreateClient(It.IsAny<string>())).Returns(client);
            return (mockFactory, mockHttpMessageHandler);
        }
    }

    public class DummyExtractionStore : IExtractionStateStore
    {
        public int DeleteRequests { get; private set; }
        public int RestoreRequests { get; private set; }
        public int StoreRequests { get; private set; }
        public Task DeleteExtractionState(IEnumerable<IExtractionState> extractionStates, string tableName, CancellationToken token)
        {
            Assert.NotNull(tableName);
            if (token.IsCancellationRequested) throw new OperationCanceledException();
            DeleteRequests++;
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public Task RestoreExtractionState<T, K>(IDictionary<string, K> extractionStates, string tableName, Action<K, T> restoreStorableState, CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            Assert.NotNull(tableName);
            if (token.IsCancellationRequested) throw new OperationCanceledException();
            RestoreRequests++;
            return Task.CompletedTask;
        }

        public Task RestoreExtractionState<K>(IDictionary<string, K> extractionStates, string tableName, bool initializeMissing, CancellationToken token) where K : BaseExtractionState
        {
            Assert.NotNull(tableName);
            if (token.IsCancellationRequested) throw new OperationCanceledException();
            RestoreRequests++;
            return Task.CompletedTask;
        }

        public Task StoreExtractionState<T, K>(IEnumerable<K> extractionStates, string tableName, Func<K, T> buildStorableState, CancellationToken token)
            where T : BaseStorableState
            where K : IExtractionState
        {
            Assert.NotNull(tableName);
            if (token.IsCancellationRequested) throw new OperationCanceledException();
            StoreRequests++;
            return Task.CompletedTask;
        }

        public Task StoreExtractionState<K>(IEnumerable<K> extractionStates, string tableName, CancellationToken token) where K : BaseExtractionState
        {
            Assert.NotNull(tableName);
            if (token.IsCancellationRequested) throw new OperationCanceledException();
            StoreRequests++;
            return Task.CompletedTask;
        }
    }
}