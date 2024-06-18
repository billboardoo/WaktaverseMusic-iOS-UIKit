//
//  LyricAPI.swift
//  LyricDomain
//
//  Created by KTH on 6/19/24.
//  Copyright © 2024 yongbeomkwak. All rights reserved.
//

import LyricDomainInterface
import BaseDomain
import ErrorModule
import Foundation
import Moya

public enum LyricAPI {
    case fetchDecoratingBackground
}

extension ArtistAPI: WMAPI {
    public var domain: WMDomain {
        switch self {
        case .fetchDecoratingBackground:
            return .lyric
        }
    }

    public var urlPath: String {
        switch self {
        case .fetchDecoratingBackground:
            return "/background"
        }
    }

    public var method: Moya.Method {
        switch self {
        case .fetchDecoratingBackground:
            return .get
        }
    }

    public var task: Moya.Task {
        switch self {
        case .fetchDecoratingBackground:
            return .requestPlain
        }
    }

    public var jwtTokenType: JwtTokenType {
        return .none
    }

    public var errorMap: [Int: WMError] {
        switch self {
        default:
            return [
                400: .badRequest,
                401: .tokenExpired,
                404: .notFound,
                429: .tooManyRequest,
                500: .internalServerError
            ]
        }
    }
}
