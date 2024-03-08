//
//  RemoteArtistDataSource.swift
//  NetworkModuleTests
//
//  Created by KTH on 2023/02/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import RxSwift
import Foundation

public protocol RemoteArtistDataSource {
    func fetchArtistList() -> Single<[ArtistListEntity]>
    func fetchArtistSongList(id: String, sort: ArtistSongSortType, page: Int) -> Single<[ArtistSongListEntity]>
}
