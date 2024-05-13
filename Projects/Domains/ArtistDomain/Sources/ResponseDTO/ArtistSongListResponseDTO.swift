//
//  ArtistSongListResponseDTO.swift
//  DataMappingModule
//
//  Created by KTH on 2023/02/01.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import ArtistDomainInterface
import Foundation
import Utility

public struct ArtistSongListResponseDTO: Decodable, Equatable {
    let songID, title: String
    let artists: [String]
    let date: Int

    public static func == (lhs: Self, rhs: Self) -> Bool {
        return lhs.songID == rhs.songID
    }

    enum CodingKeys: String, CodingKey {
        case songID = "videoId"
        case title, artists, date
    }
}

public extension ArtistSongListResponseDTO {
    func toDomain() -> ArtistSongListEntity {
        ArtistSongListEntity(
            songID: songID,
            title: title,
            artist: artists.joined(separator: ", "),
            date: date.changeDateFormat(origin: "yyMMdd", result: "yyyy.MM.dd"),
            isSelected: false
        )
    }
}
