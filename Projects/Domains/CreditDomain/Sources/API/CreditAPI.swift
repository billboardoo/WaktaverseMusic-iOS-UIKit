import BaseDomain
import CreditDomainInterface
import ErrorModule
import Foundation
import KeychainModule
import Moya

public enum CreditAPI {
    case fetchCreditSongList(
        name: String,
        order: CreditSongOrderType,
        page: Int,
        limit: Int
    )
}

extension CreditAPI: WMAPI {
    public var domain: WMDomain {
        .credit
    }

    public var urlPath: String {
        switch self {
        case let .fetchCreditSongList(name, _, _, _):
            return "/\(name)/songs"
        }
    }

    public var method: Moya.Method {
        switch self {
        case .fetchCreditSongList:
            return .get
        }
    }

    public var task: Moya.Task {
        switch self {
        case let .fetchCreditSongList(name, order, page, limit):
            return .requestParameters(parameters: [
                "order": order.rawValue,
                "page": page,
                "limit": limit
            ], encoding: URLEncoding.queryString)
        }
    }

    public var jwtTokenType: JwtTokenType {
        switch self {
        case .fetchCreditSongList:
            return .none
        }
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
