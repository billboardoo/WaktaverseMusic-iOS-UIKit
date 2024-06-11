import Foundation
import RxSwift

public protocol RemoteSongsDataSource {
    func fetchSong(id: String) -> Single<SongEntity>
    func fetchLyrics(id: String) -> Single<[LyricsEntity]>
    func fetchSongCredits(id: String) -> Single<[SongCreditsEntity]>
    func fetchNewSongs(type: NewSongGroupType, page: Int, limit: Int) -> Single<[NewSongsEntity]>
}
