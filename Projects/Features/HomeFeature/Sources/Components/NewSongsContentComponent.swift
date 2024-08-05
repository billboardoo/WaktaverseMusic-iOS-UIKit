import BaseFeature
import BaseFeatureInterface
import ChartDomainInterface
import Foundation
import NeedleFoundation
import SignInFeatureInterface
import SongsDomainInterface

public protocol NewSongsContentDependency: Dependency {
    var fetchNewSongsUseCase: any FetchNewSongsUseCase { get }
    var fetchNewSongsPlaylistUseCase: any FetchNewSongsPlaylistUseCase { get }
    var containSongsFactory: any ContainSongsFactory { get }
    var signInFactory: any SignInFactory { get }
    var textPopUpFactory: any TextPopUpFactory { get }
    var songDetailPresenter: any SongDetailPresentable { get }
}

public final class NewSongsContentComponent: Component<NewSongsContentDependency> {
    public func makeView(type: NewSongGroupType) -> NewSongsContentViewController {
        return NewSongsContentViewController.viewController(
            viewModel: .init(
                type: type,
                fetchNewSongsUseCase: dependency.fetchNewSongsUseCase,
                fetchNewSongsPlaylistUseCase: dependency.fetchNewSongsPlaylistUseCase
            ),
            containSongsFactory: dependency.containSongsFactory,
            textPopupFactory: dependency.textPopUpFactory,
            signInFactory: dependency.signInFactory,
            songDetailPresenter: dependency.songDetailPresenter
        )
    }
}
