//
//  ArtistMusicComponent.swift
//  ArtistFeature
//
//  Created by KTH on 2023/02/10.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import ArtistDomainInterface
import DomainModule
import Foundation
import NeedleFoundation

public protocol ArtistMusicDependency: Dependency {
    var artistMusicContentComponent: ArtistMusicContentComponent { get }
}

public final class ArtistMusicComponent: Component<ArtistMusicDependency> {
    public func makeView(model: ArtistListEntity?) -> ArtistMusicViewController {
        return ArtistMusicViewController.viewController(
            model: model,
            artistMusicContentComponent: dependency.artistMusicContentComponent
        )
    }
}
