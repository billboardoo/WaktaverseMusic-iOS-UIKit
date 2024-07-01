import AuthDomainInterface
import BaseFeature
import BaseFeatureInterface
import NeedleFoundation
import PlaylistDomainInterface
import PlaylistFeatureInterface
import UIKit

public protocol UnknownPlaylistDetailDependency: Dependency {
    var fetchPlaylistDetailUseCase: any FetchPlaylistDetailUseCase { get }

    var uploadPlaylistImageUseCase: any UploadPlaylistImageUseCase { get }

    var subscribePlaylistUseCase: any SubscribePlaylistUseCase { get }

    var logoutUseCase: any LogoutUseCase { get }

    var containSongsFactory: any ContainSongsFactory { get }

    var textPopUpFactory: any TextPopUpFactory { get }
}

public final class UnknownPlaylistDetailComponent: Component<UnknownPlaylistDetailDependency>,
    UnknownPlaylistDetailFactory {
    public func makeView(key: String) -> UIViewController {
        return UnknownPlaylistDetailViewController(
            reactor: UnknownPlaylistDetailReactor(
                key: key,
                fetchPlaylistDetailUseCase: dependency.fetchPlaylistDetailUseCase,
                subscribePlaylistUseCase: dependency.subscribePlaylistUseCase,
                logoutUseCase: dependency.logoutUseCase
            ),
            containSongsFactory: dependency.containSongsFactory,
            textPopUpFactory: dependency.textPopUpFactory
        )
    }
}
