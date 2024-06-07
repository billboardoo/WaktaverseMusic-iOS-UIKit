import Foundation
import PlayListDomainInterface

public struct AddSongResponseDTO: Decodable {
    public let addedSongCount: Int
    public let isDuplicatedSongsExist: Bool
}

public extension AddSongResponseDTO {
    func toDomain() -> AddSongEntity {
        AddSongEntity(
            added_songs_length: addedSongCount,
            duplicated: isDuplicatedSongsExist
        )
    }
}
