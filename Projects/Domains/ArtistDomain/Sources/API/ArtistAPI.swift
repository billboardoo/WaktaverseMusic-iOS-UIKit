import ArtistDomainInterface
import BaseDomain
import ErrorModule
import Foundation
import Moya

public enum ArtistAPI {
    case fetchArtistList
    case fetchArtistSongList(id: String, sort: ArtistSongSortType, page: Int)
    case fetchSubscriptionStatus(id: String)
    case subscriptionArtist(id: String, on: Bool)
}

extension ArtistAPI: WMAPI {
    public var domain: WMDomain {
        return .artist
    }

    public var urlPath: String {
        switch self {
        case .fetchArtistList:
            return "/list"
        case let .fetchArtistSongList(id, _, _):
            return "/\(id)/songs"
        case let .fetchSubscriptionStatus(id):
            return "/\(id)/subscription"
        case let .subscriptionArtist(id, _):
            return "/\(id)/subscription"
        }
    }

    public var method: Moya.Method {
        switch self {
        case .fetchArtistList,
             .fetchArtistSongList,
             .fetchSubscriptionStatus:
            return .get
        case let .subscriptionArtist(_, on):
            return on ? .post : .delete
        }
    }

    public var task: Moya.Task {
        switch self {
        case .fetchArtistList,
             .fetchSubscriptionStatus,
             .subscriptionArtist:
            return .requestPlain
        case let .fetchArtistSongList(_, sort, page):
            return .requestParameters(
                parameters: [
                    "type": sort.rawValue,
                    "page": page
                ],
                encoding: URLEncoding.queryString
            )
        }
    }

    public var jwtTokenType: JwtTokenType {
        switch self {
        case .fetchArtistList, .fetchArtistSongList:
            return .none
        case .fetchSubscriptionStatus, .subscriptionArtist:
            return .accessToken
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
