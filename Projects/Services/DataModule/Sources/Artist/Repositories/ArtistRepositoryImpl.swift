//
//  ArtistRepositoryImpl.swift
//  DataModule
//
//  Created by KTH on 2023/02/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import DataMappingModule
import DomainModule
import ErrorModule
import NetworkModule
import DatabaseModule
import RxSwift

public struct ArtistRepositoryImpl: ArtistRepository {
    private let remoteArtistDataSource: any RemoteArtistDataSource
    
    public init(
        remoteArtistDataSource: RemoteArtistDataSource
    ) {
        self.remoteArtistDataSource = remoteArtistDataSource
    }
    
    public func fetchArtistList() -> Single<[ArtistListEntity]> {
        remoteArtistDataSource.fetchArtistList()
    }
    
    public func fetchArtistSongList(id: String, sort: ArtistSongSortType, page: Int) -> Single<[ArtistSongListEntity]> {
        remoteArtistDataSource.fetchArtistSongList(id: id, sort: sort, page: page)
    }
}
