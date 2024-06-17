import FaqDomainInterface
import MyInfoFeatureInterface
import UIKit
import Foundation
import NeedleFoundation

public final class FaqContentComponent: Component<EmptyDependency>, FaqContentFactory {
    public func makeView(dataSource: [FaqModel]) -> UIViewController {
        return FaqContentViewController.viewController(viewModel: .init(dataSource: dataSource))
    }
}
