//
//  SearchComponent.swift
//  SearchFeature
//
//  Created by yongbeomkwak on 2023/02/10.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import DomainModule
import Foundation
import NeedleFoundation

public protocol BugReportDependency: Dependency {
    var reportBugUseCase: any ReportBugUseCase { get }
}

public final class BugReportComponent: Component<BugReportDependency> {
    public func makeView() -> BugReportViewController {
        return BugReportViewController.viewController(viewModel: .init(reportBugUseCase: dependency.reportBugUseCase))
    }
}
