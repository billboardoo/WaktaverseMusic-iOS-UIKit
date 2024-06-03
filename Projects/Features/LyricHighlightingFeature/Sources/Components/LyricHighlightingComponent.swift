//
//  LyricHighlightingComponent.swift
//  LyricHighlightingFeature
//
//  Created by KTH on 6/3/24.
//  Copyright © 2024 yongbeomkwak. All rights reserved.
//

import Foundation
import NeedleFoundation
import SongsDomainInterface

public protocol LyricHighlightingDependency: Dependency {
    var fetchLyricsUseCase: any FetchLyricsUseCase { get }
    var lyricDecoratingComponent: LyricDecoratingComponent { get }
}

public final class LyricHighlightingComponent: Component<LyricHighlightingDependency> {
    public func makeView(id: String) -> LyricHighlightingViewController {
        let viewModel = LyricHighlightingViewModel(id: id, fetchLyricsUseCase: dependency.fetchLyricsUseCase)
        return LyricHighlightingViewController(viewModel: viewModel)
    }
}
