//===----------------------------------------------------------------------===//
//
// This source file is part of the RediStack open source project
//
// Copyright (c) 2019 RediStack project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of RediStack project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

@testable import RediStack
import RediStackTestUtils
import XCTest
import Logging

final class PipelineCommandsTests: RediStackIntegrationTestCase {
    func test_setex() async throws {
        let value = try await connection.send(SETEX("1", to: "Some value", expirationInSeconds: 5)).get()
        XCTAssertEqual(value, .simpleString("OK".byteBuffer))
    }

    func test_two_setex() async throws {
        connection.sendCommandsImmediately = false
        let pr1 = connection.setex("1", to: "1", expirationInSeconds: 500)
        let pr2 = connection.setex("2", to: "2", expirationInSeconds: 500)
        connection.sendCommandsImmediately = true

        try await pr1.get()
        try await pr2.get()
    }
}

private struct Pipeline: RedisCommandSignature {

    let nested: [AnyRedisCommandSignature<RESPValue>]

    init(_ nested: any RedisCommandSignature...) {
        self.nested = nested.map {
            AnyRedisCommandSignature(commands: $0.commands) {
                $0
            }
        }
    }

    var commands: [(command: String, arguments: [RESPValue])] {
        nested.flatMap(\.commands)
    }
    
    func makeResponse(from response: RESPValue) throws -> RESPValue {
        response
    }
}

private struct SETEX: RedisCommandSignature, Equatable {
    
    public typealias Value = RESPValue
    
    public var commands: [(command: String, arguments: [RESPValue])] {
        [(
            "SETEX",
            [RESPValue(from: key), RESPValue(from: max(1, expirationInSeconds)), value]
        )]
    }
    
    public var key: RedisKey
    public var value: RESPValue
    public var expirationInSeconds: Int
    
    public init<T: RESPValueConvertible>(_ key: RedisKey, to value: T, expirationInSeconds: Int) {
        self.key = key
        self.value = value.convertedToRESPValue()
        self.expirationInSeconds = expirationInSeconds
    }
}

private struct AnyRedisCommandSignature<Value>: RedisCommandSignature {
    
    let commands: [(command: String, arguments: [RESPValue])]
    let makeResponse: (RESPValue) throws -> Value
    
    func makeResponse(from response: RESPValue) throws -> Value {
        try makeResponse(response)
    }
}
