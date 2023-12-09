import Moya
import DataMappingModule
import ErrorModule
import Foundation
import KeychainModule

public enum FaqAPI {
    case fetchFaqCategories
    case fetchFaq
}

extension FaqAPI: WMAPI {

    public var domain: WMDomain {
        .faq
    }

    public var urlPath: String {
        switch self {
        case .fetchFaqCategories:
            return "/categories"
        case .fetchFaq:
            return ""
        }
    }
        
    public var method: Moya.Method {
        switch self {
        case .fetchFaqCategories,.fetchFaq:
            return .get
        }
    }
    
    public var task: Moya.Task {
        switch self {
        case .fetchFaqCategories,.fetchFaq:
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
