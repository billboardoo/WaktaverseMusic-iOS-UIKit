import AuthDomainInterface
import AuthenticationServices
import BaseFeature
import CryptoSwift
import LogManager
import NaverThirdPartyLogin
import RxRelay
import RxSwift
import UserDomainInterface
import Utility

public final class LoginViewModel: NSObject { // 네이버 델리게이트를 받기위한 NSObject 상속
    private let fetchTokenUseCase: FetchTokenUseCase
    private let fetchUserInfoUseCase: FetchUserInfoUseCase
    let input: Input = Input()
    let output: Output = Output()
    private let disposeBag = DisposeBag()

    deinit {
        LogManager.printDebug("❌:: \(Self.self) deinit")
    }

    public struct Input {
        let didTapNaverLoginButton: PublishRelay<Void> = .init()
        let didTapAppleLoginButton: PublishRelay<Void> = .init()
        let arrivedTokenFromThirdParty: BehaviorRelay<(ProviderType, String)> = .init(value: (.apple, ""))
    }

    public struct Output {
        let showToast: PublishRelay<String> = .init()
        let dismissLoginScene: PublishRelay<ProviderType> = .init()
        let showLoading: PublishRelay<Bool> = .init()
    }

    public init(
        fetchTokenUseCase: FetchTokenUseCase,
        fetchUserInfoUseCase: FetchUserInfoUseCase
    ) {
        self.fetchTokenUseCase = fetchTokenUseCase
        self.fetchUserInfoUseCase = fetchUserInfoUseCase
        super.init()
        GoogleLoginManager.shared.googleOAuthLoginDelegate = self
        NaverThirdPartyLoginConnection.getSharedInstance()?.delegate = self
        bind()
    }
}

private extension LoginViewModel {
    func bind() {
        input.didTapNaverLoginButton
            .bind(with: self, onNext: { owner, _ in
                NaverThirdPartyLoginConnection.getSharedInstance()?.delegate = owner
                NaverThirdPartyLoginConnection.getSharedInstance()?.requestThirdPartyLogin()
            })
            .disposed(by: disposeBag)

        input.didTapAppleLoginButton
            .bind(with: self, onNext: { owner, _ in
                let appleIdProvider = ASAuthorizationAppleIDProvider()
                let request = appleIdProvider.createRequest()
                request.requestedScopes = [.fullName, .email]
                let auth = ASAuthorizationController(authorizationRequests: [request])
                auth.delegate = owner
                auth.presentationContextProvider = owner
                auth.performRequests()
            })
            .disposed(by: disposeBag)

        input.arrivedTokenFromThirdParty
            .debug("🚚:: arrivedTokenFromThirdParty")
            .filter { !$0.1.isEmpty }
            .do(onNext: { [output] _ in
                output.showLoading.accept(true)
            })
            .flatMap { [fetchTokenUseCase] provider, token in
                fetchTokenUseCase.execute(providerType: provider, token: token)
                    .catch { (error: Error) in
                        return Single.error(error)
                    }
                    .asObservable()
            }
            .flatMap { [fetchUserInfoUseCase] _ in
                fetchUserInfoUseCase.execute()
                    .catch { (error: Error) in
                        return Single.error(error)
                    }
                    .asObservable()
            }
            .subscribe(onNext: { [input, output] entity in
                LogManager.setUserID(userID: entity.id)
                PreferenceManager.shared.setUserInfo(
                    ID: entity.id,
                    platform: entity.platform,
                    profile: entity.profile,
                    name: entity.name,
                    itemCount: entity.itemCount
                )
                output.dismissLoginScene.accept(input.arrivedTokenFromThirdParty.value.0)
                output.showLoading.accept(false)

            }, onError: { [input, output] error in
                let error = error.asWMError
                output.showToast.accept(error.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                output.dismissLoginScene.accept(input.arrivedTokenFromThirdParty.value.0)
                output.showLoading.accept(false)
            })
            .disposed(by: disposeBag)
    }
}

extension LoginViewModel: GoogleOAuthLoginDelegate {
    public func requestGoogleAccessToken(_ code: String) {
        Task {
            let id = try await GoogleLoginManager.shared.getGoogleOAuthToken(code)
            input.arrivedTokenFromThirdParty.accept((.google, id))
        }
    }
}

extension LoginViewModel: NaverThirdPartyLoginConnectionDelegate {
    public func oauth20ConnectionDidFinishRequestACTokenWithAuthCode() {
        let shared = NaverThirdPartyLoginConnection.getSharedInstance()
        guard let accessToken = shared?.accessToken else { return }
        input.arrivedTokenFromThirdParty.accept((.naver, accessToken))
    }

    public func oauth20ConnectionDidFinishRequestACTokenWithRefreshToken() {
        let shared = NaverThirdPartyLoginConnection.getSharedInstance()
        guard let accessToken = shared?.accessToken else { return }
        input.arrivedTokenFromThirdParty.accept((.naver, accessToken))
    }

    public func oauth20ConnectionDidFinishDeleteToken() {
        LogManager.printDebug("oauth20ConnectionDidFinishDeleteToken")
    }

    public func oauth20Connection(_ oauthConnection: NaverThirdPartyLoginConnection!, didFailWithError error: Error!) {
        output.showToast.accept(error.localizedDescription)
    }
}

extension LoginViewModel: ASAuthorizationControllerDelegate, ASAuthorizationControllerPresentationContextProviding {
    public func presentationAnchor(for controller: ASAuthorizationController) -> ASPresentationAnchor {
        return (UIApplication.shared.connectedScenes.first as? UIWindowScene)?.windows.first ?? .init()
    }

    public func authorizationController(
        controller: ASAuthorizationController,
        didCompleteWithAuthorization authorization: ASAuthorization
    ) {
        if let credential = authorization.credential as? ASAuthorizationAppleIDCredential,
           let rawData = credential.identityToken {
            let token = String(decoding: rawData, as: UTF8.self)
            input.arrivedTokenFromThirdParty.accept((.apple, token))
        }
    }

    public func authorizationController(controller: ASAuthorizationController, didCompleteWithError error: Error) {
        output.showToast.accept(error.localizedDescription)
    }
}
