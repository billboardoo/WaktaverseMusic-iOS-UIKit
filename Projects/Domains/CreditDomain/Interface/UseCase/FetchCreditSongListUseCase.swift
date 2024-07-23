import Foundation
import RxSwift
import SongsDomainInterface

public enum CreditSongOrderType: String {
    case latest
    case popular
    case oldest
}

public protocol FetchCreditSongListUseCase {
    func execute(
        name: String,
        order: CreditSongOrderType,
        page: Int,
        limit: Int
    ) -> Single<[SongEntity]>
}

public extension FetchCreditSongListUseCase {
    func execute(
        name: String,
        order: CreditSongOrderType,
        page: Int
    ) -> Single<[SongEntity]> {
        self.execute(name: name, order: order, page: page, limit: 50)
    }
}
