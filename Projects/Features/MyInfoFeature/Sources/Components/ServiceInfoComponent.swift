import BaseFeatureInterface
import Foundation
import MyInfoFeatureInterface
import NeedleFoundation

public protocol ServiceInfoDependency: Dependency {
    var openSourceLicenseFactory: any OpenSourceLicenseFactory { get }
    var textPopUpFactory: any TextPopUpFactory { get }
}

public final class ServiceInfoComponent: Component<ServiceInfoDependency> {
    public func makeView() -> ServiceInfoViewController {
        return ServiceInfoViewController.viewController(
            viewModel: ServiceInfoViewModel(),
            openSourceLicenseFactory: dependency.openSourceLicenseFactory,
            textPopUpFactory: dependency.textPopUpFactory
        )
    }
}
