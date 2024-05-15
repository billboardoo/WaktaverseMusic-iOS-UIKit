//
//  LyricsResponseDTO.swift
//  DataMappingModule
//
//  Created by YoungK on 2023/02/22.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import SongsDomainInterface

public struct LyricsResponseDTO: Decodable {
    let text: String
}

public extension LyricsResponseDTO {
    func toDomain() -> LyricsEntity {
        return LyricsEntity(text: text)
    }
}
