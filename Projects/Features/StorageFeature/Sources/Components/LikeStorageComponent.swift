import AuthDomainInterface
import BaseDomainInterface
import BaseFeature
import BaseFeatureInterface
import Foundation
import NeedleFoundation
import SignInFeatureInterface
import UIKit
import UserDomainInterface

public protocol LikeStorageDependency: Dependency {
    var containSongsFactory: any ContainSongsFactory { get }
    var fetchFavoriteSongsUseCase: any FetchFavoriteSongsUseCase { get }
    var editFavoriteSongsOrderUseCase: any EditFavoriteSongsOrderUseCase { get }
    var deleteFavoriteListUseCase: any DeleteFavoriteListUseCase { get }
    var logoutUseCase: any LogoutUseCase { get }
    var textPopUpFactory: any TextPopUpFactory { get }
    var signInFactory: any SignInFactory { get }
    var songDetailPresenter: any SongDetailPresentable { get }
}

public final class LikeStorageComponent: Component<LikeStorageDependency> {
    public func makeView() -> UIViewController {
        return LikeStorageViewController.viewController(
            reactor: LikeStorageReactor(
                storageCommonService: DefaultStorageCommonService.shared,
                fetchFavoriteSongsUseCase: dependency.fetchFavoriteSongsUseCase,
                deleteFavoriteListUseCase: dependency.deleteFavoriteListUseCase,
                editFavoriteSongsOrderUseCase: dependency.editFavoriteSongsOrderUseCase
            ),
            containSongsFactory: dependency.containSongsFactory,
            textPopUpFactory: dependency.textPopUpFactory,
            signInFactory: dependency.signInFactory,
            songDetailPresenter: dependency.songDetailPresenter
        )
    }
}
