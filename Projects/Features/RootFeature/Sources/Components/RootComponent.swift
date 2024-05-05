import AppDomainInterface
import AuthDomainInterface
import BaseFeatureInterface
import MainTabFeature
import NeedleFoundation
import UIKit
import UserDomainInterface

public protocol RootDependency: Dependency {
    var mainContainerComponent: MainContainerComponent { get }
    var permissionComponent: PermissionComponent { get }
    var fetchUserInfoUseCase: any FetchUserInfoUseCase { get }
    var fetchAppCheckUseCase: any FetchAppCheckUseCase { get }
    var logoutUseCase: any LogoutUseCase { get }
    var checkIsExistAccessTokenUseCase: any CheckIsExistAccessTokenUseCase { get }
    var textPopUpFactory: any TextPopUpFactory { get }
}

public final class RootComponent: Component<RootDependency> {
    public func makeView() -> IntroViewController {
        return IntroViewController.viewController(
            mainContainerComponent: dependency.mainContainerComponent,
            permissionComponent: dependency.permissionComponent,
            textPopUpFactory: dependency.textPopUpFactory,
            viewModel: IntroViewModel(
                fetchUserInfoUseCase: dependency.fetchUserInfoUseCase,
                fetchAppCheckUseCase: dependency.fetchAppCheckUseCase,
                logoutUseCase: dependency.logoutUseCase,
                checkIsExistAccessTokenUseCase: dependency.checkIsExistAccessTokenUseCase
            )
        )
    }
}
