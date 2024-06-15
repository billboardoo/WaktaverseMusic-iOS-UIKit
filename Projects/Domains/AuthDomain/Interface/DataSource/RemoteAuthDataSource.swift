import Foundation
import RxSwift

public protocol RemoteAuthDataSource {
    func fetchToken(providerType: ProviderType, token: String) -> Single<AuthLoginEntity>
    func reGenerateAccessToken() -> Single<AuthLoginEntity>
    func fetchNaverUserInfo(tokenType: String, accessToken: String) -> Single<NaverUserInfoEntity>
}
