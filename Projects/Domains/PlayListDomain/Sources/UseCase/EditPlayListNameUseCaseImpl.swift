//
//  FetchArtistListUseCaseImpl.swift
//  DataModule
//
//  Created by KTH on 2023/02/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import PlayListDomainInterface
import Foundation
import RxSwift

public struct EditPlayListNameUseCaseImpl: EditPlayListNameUseCase {
    private let playListRepository: any PlayListRepository

    public init(
        playListRepository: PlayListRepository
    ) {
        self.playListRepository = playListRepository
    }

    public func execute(key: String, title: String) -> Single<EditPlayListNameEntity> {
        playListRepository.editPlayListName(key: key, title: title)
    }
}
