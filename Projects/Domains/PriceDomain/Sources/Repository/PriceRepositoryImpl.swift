import Foundation
import RxSwift
import PriceDomainInterface

public final class PriceRepositoryImpl: PriceRepository {
    
    private let remotePriceDataSource: any RemotePriceDataSource

    public init(
        remotePriceDataSource: any RemotePriceDataSource
    ) {
        self.remotePriceDataSource = remotePriceDataSource
    }
    
    public func fetchPlaylistCreationPrice() -> Single<PriceEntity> {
        remotePriceDataSource.fetchPlaylistCreationPrice()
    }
    
    public func fetchPlaylistImagePrice() -> Single<PriceEntity> {
        remotePriceDataSource.fetchPlaylistImagePrice()
    }


}
