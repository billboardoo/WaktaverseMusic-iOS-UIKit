import Moya
import DataMappingModule
import ErrorModule
import Foundation
import KeychainModule

public enum UserAPI {
    case fetchProfileList
    case setProfile(image: String)
    case setUserName(name: String)
    case fetchSubPlayList
    case fetchFavoriteSongs
}

public struct RequsetProfileModel:Encodable {
    var image:String
}

public struct RequsetUserNameModel:Encodable {
    var username:String
}

extension UserAPI: WMAPI {

    public var domain: WMDomain {
        .user
    }

    public var urlPath: String {
        switch self {
        case .fetchProfileList:
            return "/profile/list"
        case .setProfile:
            return "/profile/set"
        case .setUserName:
            return "/username"
        case .fetchSubPlayList:
            return "/playlists"
        case .fetchFavoriteSongs:
            return "/likes"
        }
    }
        
    public var method: Moya.Method {
        switch self {
        case .setProfile, .setUserName:
            return .post
        case .fetchProfileList, .fetchSubPlayList,.fetchFavoriteSongs:
            return .get
        }
    }
    
    public var task: Moya.Task {
        switch self {
        case let .setProfile(image):
            return .requestJSONEncodable(RequsetProfileModel(image: image))
        case let .setUserName(name):
            return .requestJSONEncodable(RequsetUserNameModel(username: name))
        case .fetchProfileList, .fetchSubPlayList,.fetchFavoriteSongs:
            return .requestPlain
        }
    }

    
        
    public var jwtTokenType: JwtTokenType {
        return .accessToken
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
