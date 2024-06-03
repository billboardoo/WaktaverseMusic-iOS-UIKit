//
//  ArtistComponent.swift
//  ArtistFeatureTests
//
//  Created by KTH on 2023/02/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import ArtistDomainInterface
import Foundation
import LyricHighlightingFeature
import NeedleFoundation

public protocol ArtistDependency: Dependency {
    var fetchArtistListUseCase: any FetchArtistListUseCase { get }
    var artistDetailComponent: ArtistDetailComponent { get }
    var lyricHighlightingComponent: LyricHighlightingComponent { get }
}

public final class ArtistComponent: Component<ArtistDependency> {
    public func makeView() -> ArtistViewController {
        let reactor = ArtistReactor(fetchArtistListUseCase: dependency.fetchArtistListUseCase)
        return ArtistViewController.viewController(
            reactor: reactor,
            artistDetailComponent: dependency.artistDetailComponent,
            lyricHighlightingComponent: dependency.lyricHighlightingComponent
        )
    }
}
