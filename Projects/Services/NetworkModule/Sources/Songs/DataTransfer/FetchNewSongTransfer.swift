//
//  FetchNewSongTransfer.swift
//  NetworkModule
//
//  Created by KTH on 2023/02/20.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import DataMappingModule
import DomainModule
import Utility

public extension NewSongResponseDTO {
    func toDomain() -> NewSongEntity {
        NewSongEntity(
            id: id,
            title: title,
            artist: artist,
            remix: remix,
            reaction: reaction,
            views: views,
            last: last,
            date: date
        )
    }
}
