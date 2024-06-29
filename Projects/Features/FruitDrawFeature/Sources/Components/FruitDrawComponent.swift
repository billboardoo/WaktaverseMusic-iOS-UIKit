import AuthDomainInterface
import BaseFeatureInterface
import Foundation
import FruitDrawFeatureInterface
import NeedleFoundation
import UIKit
import UserDomainInterface

public protocol FruitDrawDependency: Dependency {
    var fetchFruitDrawStatusUseCase: any FetchFruitDrawStatusUseCase { get }
    var drawFruitUseCase: any DrawFruitUseCase { get }
    var logoutUseCase: any LogoutUseCase { get }
    var textPopUpFactory: any TextPopUpFactory { get }
}

public final class FruitDrawComponent: Component<FruitDrawDependency>, FruitDrawFactory {
    public func makeView(delegate: any FruitDrawViewControllerDelegate) -> UIViewController {
        return FruitDrawViewController(
            viewModel: .init(
                fetchFruitDrawStatusUseCase: dependency.fetchFruitDrawStatusUseCase,
                drawFruitUseCase: dependency.drawFruitUseCase,
                logoutUseCase: dependency.logoutUseCase
            ),
            textPopUpFactory: dependency.textPopUpFactory,
            delegate: delegate
        )
    }
}
