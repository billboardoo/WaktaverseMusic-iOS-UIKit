import CreditDomain
import CreditDomainInterface
import CreditSongListFeature
import CreditSongListFeatureInterface
import SongCreditFeature
import SongCreditFeatureInterface

public extension AppComponent {
    var remoteCreditDataSource: any RemoteCreditDataSource {
        shared {
            RemoteCreditDataSourceImpl(keychain: keychain)
        }
    }

    var creditRepository: any CreditRepository {
        shared {
            CreditRepositoryImpl(remoteCreditDataSource: remoteCreditDataSource)
        }
    }

    var fetchCreditSongListUseCase: any FetchCreditSongListUseCase {
        shared {
            FetchCreditSongListUseCaseImpl(creditRepository: creditRepository)
        }
    }

    var fetchCreditProfileImageURLUseCase: any FetchCreditProfileImageURLUseCase {
        shared {
            FetchCreditProfileImageURLUseCaseImpl(creditRepository: creditRepository)
        }
    }

    var songCreditFactory: any SongCreditFactory {
        SongCreditComponent(parent: self)
    }

    var creditSongListFactory: any CreditSongListFactory {
        CreditSongListComponent(parent: self)
    }

    var creditSongListTabFactory: any CreditSongListTabFactory {
        CreditSongListTabComponent(parent: self)
    }

    var creditSongListTabItemFactory: any CreditSongListTabItemFactory {
        CreditSongListTabItemComponent(parent: self)
    }
}
