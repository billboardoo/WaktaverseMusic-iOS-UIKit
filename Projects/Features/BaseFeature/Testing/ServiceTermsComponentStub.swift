import BaseFeatureInterface
import BaseFeature
import UIKit

public final class ServiceTermsStub: ServiceTermFactory {
    public func makeView() -> UIViewController {
        return ContractViewController.viewController(type: .service)
    }
}
