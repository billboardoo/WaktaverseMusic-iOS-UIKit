import FirebaseMessaging
import Foundation
import LogManager
import NoticeDomainInterface
import NotificationDomainInterface
import RxRelay
import RxSwift
import Utility

private typealias Observer = (
    detectedRefreshPushToken: Void,
    isLoggedIn: Bool,
    grantedNotificationAuthorization: Bool
)

public final class MainTabBarViewModel {
    private let fetchNoticePopupUseCase: FetchNoticePopupUseCase
    private let fetchNoticeIDListUseCase: FetchNoticeIDListUseCase
    private let updateNotificationTokenUseCase: UpdateNotificationTokenUseCase
    private let disposeBag = DisposeBag()

    public init(
        fetchNoticePopupUseCase: any FetchNoticePopupUseCase,
        fetchNoticeIDListUseCase: any FetchNoticeIDListUseCase,
        updateNotificationTokenUseCase: any UpdateNotificationTokenUseCase
    ) {
        self.fetchNoticePopupUseCase = fetchNoticePopupUseCase
        self.fetchNoticeIDListUseCase = fetchNoticeIDListUseCase
        self.updateNotificationTokenUseCase = updateNotificationTokenUseCase
    }

    public struct Input {
        let fetchNoticePopup: PublishSubject<Void> = PublishSubject()
        let fetchNoticeIDList: PublishSubject<Void> = PublishSubject()
        let detectedRefreshPushToken: PublishSubject<Void> = PublishSubject()
    }

    public struct Output {
        let noticePopupDataSource: BehaviorRelay<[FetchNoticeEntity]> = BehaviorRelay(value: [])
    }

    public func transform(from input: Input) -> Output {
        let output = Output()
        let ignoredPopupIDs: [Int] = Utility.PreferenceManager.ignoredPopupIDs ?? []
        DEBUG_LOG("ignoredPopupIDs: \(ignoredPopupIDs)")

        input.fetchNoticePopup
            .flatMap { [fetchNoticePopupUseCase] _ -> Single<[FetchNoticeEntity]> in
                return fetchNoticePopupUseCase.execute()
                    .catchAndReturn([])
            }
            .map { entities in
                guard !ignoredPopupIDs.isEmpty else { return entities }
                return entities.filter { entity in
                    return !ignoredPopupIDs.contains(where: { $0 == entity.id })
                }
            }
            .debug("ignoredPopupIDs")
            .bind(to: output.noticePopupDataSource)
            .disposed(by: disposeBag)

        input.fetchNoticeIDList
            .withLatestFrom(PreferenceManager.$readNoticeIDs)
            .filter { ($0 ?? []).isEmpty }
            .flatMap { [fetchNoticeIDListUseCase] _ -> Single<FetchNoticeIDListEntity> in
                return fetchNoticeIDListUseCase.execute()
                    .catchAndReturn(FetchNoticeIDListEntity(status: "404", data: []))
            }
            .map { $0.data }
            .bind { allNoticeIDs in
                PreferenceManager.readNoticeIDs = allNoticeIDs
            }
            .disposed(by: disposeBag)

        // 호출 조건: 앱 실행 시 1회, 리프레쉬 토큰 감지, 기기알림 on/off
        Observable.combineLatest(
            input.detectedRefreshPushToken,
            PreferenceManager.$userInfo.map { $0?.ID }.distinctUntilChanged(),
            PreferenceManager.$pushNotificationAuthorizationStatus.distinctUntilChanged().map { $0 ?? false }
        ) { detected, id, granted -> Observer in
            return Observer(
                detectedRefreshPushToken: detected,
                isLoggedIn: id != nil,
                grantedNotificationAuthorization: granted
            )
        }
        .flatMap { [updateNotificationTokenUseCase] observer -> Observable<Bool> in
            let updateUseCase = updateNotificationTokenUseCase.execute(type: .update)
                .debug("🔔:: updateNotificationTokenUseCase")
                .andThen(Observable.just(true))
                .catchAndReturn(false)
            let deleteUseCase = updateNotificationTokenUseCase.execute(type: .delete)
                .debug("🔔:: updateNotificationTokenUseCase")
                .andThen(Observable.just(true))
                .catchAndReturn(false)

            if observer.isLoggedIn && observer.grantedNotificationAuthorization {
                return updateUseCase

            } else if observer.isLoggedIn && observer.grantedNotificationAuthorization == false {
                return Messaging.messaging().fetchRxPushToken()
                    .asObservable()
                    .catchAndReturn("")
                    .flatMap { token in
                        return token.isEmpty ? Observable.just(false) : deleteUseCase
                    }

            } else {
                return Observable.just(false)
            }
        }
        .debug("🔔:: updateNotificationTokenUseCase")
        .subscribe()
        .disposed(by: disposeBag)

        return output
    }
}
