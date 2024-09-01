//===----------------------------------------------------------------------===//
//
// This source file is part of the RediStack open source project
//
// Copyright (c) 2019-2020 RediStack project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of RediStack project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Metrics
import NIOConcurrencyHelpers

/// The system funnel for all `Metrics` interactions from the Redis library.
///
/// It is highly recommended to not interact with this directly, and to let the library
/// use it how it sees fit.
///
/// There is a nested enum type of `RedisMetrics.Label` that is available to query, match, etc. the
/// labels used for all of the `Metrics` types created by the Redis library.
public struct RedisMetrics {
    /// An enumeration of all the labels used by the Redis library for various `Metrics` data points.
    ///
    /// Each is backed by a raw string, and this type is `CustomStringConvertible` to receive a
    /// namespaced description in the form of `"RediStack.<rawValue>"`.
    public enum Label: String, CustomStringConvertible {
        case totalConnectionCount
        case activeConnectionCount
        case activeChannelSubscriptions
        case activePatternSubscriptions
        case subscriptionMessagesReceivedCount
        case commandSuccessCount
        case commandFailureCount
        case commandRoundTripTime

        public var description: String {
            return "RediStack.\(self.rawValue)"
        }
    }

    /// The wrapped `Metrics.Gauge` maintaining the current number of connections this library has active.
    public static var activeConnectionCount = IncrementalGauge(.activeConnectionCount)
    /// The wrapped `Metrics.Gauge` maintaining the current number of subscriptions to channels.
    public static var activeChannelSubscriptions = IncrementalGauge(.activeChannelSubscriptions)
    /// The wrapped `Metrics.Gauge` maintaining the current number of subscriptions to channel patterns.
    public static var activePatternSubscriptions = IncrementalGauge(.activePatternSubscriptions)
    /// The `Metrics.Counter` that retains the number of connections made since application startup.
    public static let totalConnectionCount = Counter(label: .totalConnectionCount)
    /// The `Metrics.Counter` that retains the number of subscription messages that have been received.
    public static let subscriptionMessagesReceivedCount = Counter(label: .subscriptionMessagesReceivedCount)
    /// The `Metrics.Counter` that retains the number of commands that successfully returned from Redis
    /// since application startup.
    public static let commandSuccessCount = Counter(label: .commandSuccessCount)
    /// The `Metrics.Counter` that retains the number of commands that failed from errors returned
    /// by Redis since application startup.
    public static let commandFailureCount = Counter(label: .commandFailureCount)
    /// The `Metrics.Timer` that receives command response times in nanoseconds from when a command
    /// is first sent through the `NIO.Channel`, to when the response is first resolved.
    public static let commandRoundTripTime = Timer(label: .commandRoundTripTime)

    /// A flag to enable or disable the reporting of metrics.
    public static var reportMetrics: Bool {
        get { return _reportMetrics.load(ordering: .relaxed) }
        set { _reportMetrics.store(newValue, ordering: .relaxed) }
    }
    private static let _reportMetrics = ManagedAtomic<Bool>(true)

    private init() { }
}

extension RedisMetrics {
    /// A specialized wrapper class for working with `Metrics.Gauge` objects for the purpose of an incrementing or decrementing count of active objects.
    public class IncrementalGauge {
        private let gauge: Gauge
        private let count = ManagedAtomic<Int>(0)
        
        /// The number of the objects that are currently reported as active.
        public var currentCount: Int { return count.load(ordering: .sequentiallyConsistent) }
        
        internal init(_ label: Label) {
            self.gauge = .init(label: label)
        }
        
        /// Increments the current count by the amount specified.
        /// - Parameter amount: The number to increase the current count by. Default is `1`.
        public func increment(by amount: Int = 1) {
            guard RedisMetrics.reportMetrics else { return }
            self.count.wrappingIncrement(by: amount, ordering: .sequentiallyConsistent)
            self.gauge.record(self.count.load(ordering: .sequentiallyConsistent))
        }
        
        /// Decrements the current count by the amount specified.
        /// - Parameter amount: The number to decrease the current count by. Default is `1`.
        public func decrement(by amount: Int = 1) {
            guard RedisMetrics.reportMetrics else { return }
            self.count.wrappingDecrement(by: amount, ordering: .sequentiallyConsistent)
            self.gauge.record(self.count.load(ordering: .sequentiallyConsistent))
        }
        
        /// Resets the current count to `0`.
        public func reset() {
            guard RedisMetrics.reportMetrics else { return }
            _ = self.count.exchange(0, ordering: .sequentiallyConsistent)
            self.gauge.record(self.count.load(ordering: .sequentiallyConsistent))
        }
    }
    
    fileprivate final class _Counter: CounterHandler {

        let handler: CounterHandler
    
        init(label: Label) {
            self.handler = MetricsSystem.factory.makeCounter(
                label: label.description,
                dimensions: []
            )
        }
        
        func increment(by value: Int64) {
            guard RedisMetrics.reportMetrics else { return }
            handler.increment(by: value)
        }
        
        func reset() {
            guard RedisMetrics.reportMetrics else { return }
            handler.reset()
        }
    }

    fileprivate final class _Timer: TimerHandler {

        let handler: TimerHandler

        init(label: Label) {
            self.handler = MetricsSystem.factory.makeTimer(
                label: label.description,
                dimensions: []
            )
        }

        func recordNanoseconds(_ duration: Int64) {
            guard RedisMetrics.reportMetrics else { return }
            handler.recordNanoseconds(duration)
        }
    }
}

// MARK: SwiftMetrics Convenience

extension Metrics.Counter {
    @inline(__always)
    convenience init(label: RedisMetrics.Label) {
        self.init(
            label: label.description,
            dimensions: [],
            handler: RedisMetrics._Counter(label: label)
        )
    }
}

extension Metrics.Gauge {
    @inline(__always)
    convenience init(label: RedisMetrics.Label) {
        self.init(label: label.description)
    }
}

extension Metrics.Timer {
    @inline(__always)
    convenience init(label: RedisMetrics.Label) {
        self.init(
            label: label.description,
            dimensions: [],
            handler: RedisMetrics._Timer(label: label)
        )
    }
}
