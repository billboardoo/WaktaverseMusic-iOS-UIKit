import AuthDomainInterface
import BaseDomainInterface
import BaseFeature
import Foundation
import LogManager
import ReactorKit
import RxCocoa
import RxRelay
import RxSwift
import SongsDomainInterface
import UserDomainInterface
import Utility

final class LikeStorageReactor: Reactor {
    enum Action {
        case viewDidLoad
        case refresh
        case itemMoved(ItemMovedEvent)
        case songDidTap(Int)
        case tapAll(isSelecting: Bool)
        case playDidTap(song: SongEntity)
        case addToPlaylistButtonDidTap // 노래담기
        case presentAddToPlaylistPopup
        case addToCurrentPlaylistButtonDidTap // 재생목록추가
        case deleteButtonDidTap
        case confirmDeleteButtonDidTap
        case loginButtonDidTap
    }

    enum Mutation {
        case clearDataSource
        case updateDataSource([LikeSectionModel])
        case updateBackupDataSource([LikeSectionModel])
        case undoDataSource
        case switchEditingState(Bool)
        case updateIsLoggedIn(Bool)
        case updateIsShowActivityIndicator(Bool)
        case showToast(String)
        case showLoginAlert
        case showAddToPlaylistPopup([String])
        case showDeletePopup(Int)
        case hideSongCart
        case updateSelectedItemCount(Int)
        case updateOrder([FavoriteSongEntity])
    }

    struct State {
        var isLoggedIn: Bool
        var isEditing: Bool
        var dataSource: [LikeSectionModel]
        var backupDataSource: [LikeSectionModel]
        var selectedItemCount: Int
        var isShowActivityIndicator: Bool
        @Pulse var showAddToPlaylistPopup: [String]?
        @Pulse var showToast: String?
        @Pulse var hideSongCart: Void?
        @Pulse var showDeletePopup: Int?
        @Pulse var showLoginAlert: Void?
    }

    var initialState: State
    private let storageCommonService: any StorageCommonService
    private let fetchFavoriteSongsUseCase: any FetchFavoriteSongsUseCase
    private let deleteFavoriteListUseCase: any DeleteFavoriteListUseCase
    private let editFavoriteSongsOrderUseCase: any EditFavoriteSongsOrderUseCase
    
    init(
        storageCommonService: any StorageCommonService = DefaultStorageCommonService.shared,
        fetchFavoriteSongsUseCase: any FetchFavoriteSongsUseCase,
        deleteFavoriteListUseCase: any DeleteFavoriteListUseCase,
        editFavoriteSongsOrderUseCase: any EditFavoriteSongsOrderUseCase
    ) {
        self.initialState = State(
            isLoggedIn: false,
            isEditing: false,
            dataSource: [],
            backupDataSource: [],
            selectedItemCount: 0,
            isShowActivityIndicator: false
        )

        self.storageCommonService = storageCommonService
        self.fetchFavoriteSongsUseCase = fetchFavoriteSongsUseCase
        self.deleteFavoriteListUseCase = deleteFavoriteListUseCase
        self.editFavoriteSongsOrderUseCase = editFavoriteSongsOrderUseCase
    }

    deinit {
        LogManager.printDebug("❌ Deinit \(Self.self)")
    }

    func mutate(action: Action) -> Observable<Mutation> {
        switch action {
        case .viewDidLoad:
            return viewDidLoad()
            
        case .refresh:
            return fetchDataSource()
            
        case let .itemMoved((sourceIndex, destinationIndex)):
            return updateOrder(src: sourceIndex.row, dest: destinationIndex.row)
            
        case let .songDidTap(index):
            return changeSelectingState(index)
            
        case let .playDidTap(song):
            return playWithAddToCurrentPlaylist()
            
        case let .tapAll(isSelecting):
            return tapAll(isSelecting)
            
        case .addToPlaylistButtonDidTap:
            return showAddToPlaylistPopup()
            
        case .addToCurrentPlaylistButtonDidTap:
            return addToCurrentPlaylist()
            
        case .deleteButtonDidTap:
            let itemCount = currentState.selectedItemCount
            return .just(.showDeletePopup(itemCount))
            
        case .confirmDeleteButtonDidTap:
            return deleteSongs()
        
        case .loginButtonDidTap:
            return .just(.showLoginAlert)
            
        case .presentAddToPlaylistPopup:
            return presentAddToPlaylistPopup()
        }
    }

    func transform(mutation: Observable<Mutation>) -> Observable<Mutation> {
        let switchEditingStateMutation = storageCommonService.isEditingState
            .skip(1)
            .withUnretained(self)
            .flatMap { owner, editingState -> Observable<Mutation> in
                // 편집이 종료될 때 처리
                if editingState == false {
                    let new = owner.currentState.dataSource.flatMap { $0.items }.map { $0.song.id }
                    let original = owner.currentState.backupDataSource.flatMap { $0.items }.map { $0.song.id }
                    let isChanged = new != original
                    if isChanged {
                        return .concat(
                            .just(.updateIsShowActivityIndicator(true)),
                            owner.mutateEditSongsOrderUseCase(),
                            .just(.updateIsShowActivityIndicator(false)),
                            .just(.updateSelectedItemCount(0)),
                            .just(.hideSongCart),
                            .just(.switchEditingState(false))
                        )
                    } else {
                        return .concat(
                            .just(.updateSelectedItemCount(0)),
                            .just(.undoDataSource),
                            .just(.hideSongCart),
                            .just(.switchEditingState(false))
                        )
                    }
                } else {
                    return .just(.switchEditingState(editingState))
                }
            }

        let changedUserInfoMutation = storageCommonService.changedUserInfoEvent
            .withUnretained(self)
            .flatMap { owner, userInfo -> Observable<Mutation> in
                .concat(
                    owner.updateIsLoggedIn(userInfo),
                    owner.fetchDataSource()
                )
            }
        
        return Observable.merge(mutation, switchEditingStateMutation, changedUserInfoMutation)
    }
    
    func reduce(state: State, mutation: Mutation) -> State {
        var newState = state

        switch mutation {
        case let .updateDataSource(dataSource):
            newState.dataSource = dataSource
        case let .switchEditingState(flag):
            newState.isEditing = flag
        case let .updateOrder(dataSource):
            newState.dataSource = [LikeSectionModel(model: 0, items: dataSource)]
        case .clearDataSource:
            newState.dataSource = []
        case let .updateBackupDataSource(dataSource):
            newState.backupDataSource = dataSource
        case .undoDataSource:
            newState.dataSource = currentState.backupDataSource
        case let .updateIsLoggedIn(isLoggedIn):
            newState.isLoggedIn = isLoggedIn
        case let .updateIsShowActivityIndicator(isShow):
            newState.isShowActivityIndicator = isShow
        case let .showToast(isShow):
            newState.showToast = isShow
        case let .showAddToPlaylistPopup(selectedItemIDs):
            newState.showAddToPlaylistPopup = selectedItemIDs
        case let .showDeletePopup(itemCount):
            newState.showDeletePopup = itemCount
        case .hideSongCart:
            newState.hideSongCart = ()
        case let .updateSelectedItemCount(count):
            newState.selectedItemCount = count
        case .showLoginAlert:
            newState.showLoginAlert = ()
        }

        return newState
    }
}

extension LikeStorageReactor {
    func viewDidLoad() -> Observable<Mutation> {
        if currentState.isLoggedIn {
            return .concat(
                .just(.updateIsShowActivityIndicator(true)),
                fetchDataSource(),
                .just(.updateIsShowActivityIndicator(false))
            )
        } else {
            return .empty()
        }
    }

    func fetchDataSource() -> Observable<Mutation> {
        fetchFavoriteSongsUseCase
            .execute()
            .catchAndReturn([])
            .asObservable()
            .map { [LikeSectionModel(model: 0, items: $0)] }
            .flatMap { fetchedDataSource -> Observable<Mutation> in
                .concat(
                    .just(.updateDataSource(fetchedDataSource)),
                    .just(.updateBackupDataSource(fetchedDataSource))
                )
            }
    }
    
    func clearDataSource() -> Observable<Mutation> {
        return .just(.clearDataSource)
    }

    func deleteSongs() -> Observable<Mutation> {
        let selectedItemIDs = currentState.dataSource.flatMap { $0.items.filter { $0.isSelected == true } }.map { $0.song.id }
        storageCommonService.isEditingState.onNext(false)
        return .concat(
            .just(.updateIsShowActivityIndicator(true)),
            mutateDeleteSongsUseCase(selectedItemIDs),
            .just(.updateIsShowActivityIndicator(false)),
            .just(.hideSongCart),
            .just(.switchEditingState(false))
        )
    }
    
    func showAddToPlaylistPopup() -> Observable<Mutation> {
        let selectedItemIDs = currentState.dataSource.flatMap { $0.items.filter { $0.isSelected == true } }.map { $0.song.id }
        return .just(.showAddToPlaylistPopup(selectedItemIDs))
    }
    
    func presentAddToPlaylistPopup() -> Observable<Mutation> {
        guard var tmp = currentState.dataSource.first?.items else {
            LogManager.printError("favorite datasource is empty")
            return .empty()
        }
        for index in tmp.indices { tmp[index].isSelected = false }
        
        storageCommonService.isEditingState.onNext(false)
        return .concat(
            .just(.updateDataSource([LikeSectionModel(model: 0, items: tmp)])),
            .just(.updateSelectedItemCount(0))
        )
    }
    
    func addToCurrentPlaylist() -> Observable<Mutation> {
        #warning("PlayState 리팩토링 끝나면 수정 예정")
        return .just(.showToast("개발이 필요해요"))
    }

    func playWithAddToCurrentPlaylist() -> Observable<Mutation> {
        #warning("PlayState 리팩토링 끝나면 수정 예정")
        return .just(.showToast("개발이 필요해요"))
    }

    func updateIsLoggedIn(_ userInfo: UserInfo?) -> Observable<Mutation> {
        return .just(.updateIsLoggedIn(userInfo != nil))
    }
    
    /// 순서 변경
    func updateOrder(src: Int, dest: Int) -> Observable<Mutation> {
        guard var tmp = currentState.dataSource.first?.items else {
            LogManager.printError("favorite datasource is empty")
            return .empty()
        }

        let target = tmp[src]
        tmp.remove(at: src)
        tmp.insert(target, at: dest)
        return .just(.updateOrder(tmp))
    }

    func changeSelectingState(_ index: Int) -> Observable<Mutation> {
        guard var tmp = currentState.dataSource.first?.items else {
            LogManager.printError("favorite datasource is empty")
            return .empty()
        }

        var count = currentState.selectedItemCount
        let target = tmp[index]
        count = target.isSelected ? count - 1 : count + 1
        tmp[index].isSelected = !tmp[index].isSelected

        return .concat(
            .just(.updateDataSource([LikeSectionModel(model: 0, items: tmp)])),
            .just(.updateSelectedItemCount(count))
        )
    }

    /// 전체 곡 선택 / 해제
    func tapAll(_ flag: Bool) -> Observable<Mutation> {
        guard var tmp = currentState.dataSource.first?.items else {
            LogManager.printError("favorite datasource is empty")
            return .empty()
        }

        let count = flag ? tmp.count : 0

        for i in 0 ..< tmp.count {
            tmp[i].isSelected = flag
        }

        return .concat(
            .just(.updateDataSource([LikeSectionModel(model: 0, items: tmp)])),
            .just(.updateSelectedItemCount(count))
        )
    }
}

private extension LikeStorageReactor {
    func mutateDeleteSongsUseCase(_ ids: [String]) -> Observable<Mutation> {
        deleteFavoriteListUseCase.execute(ids: ids)
            .asObservable()
            .withUnretained(self)
            .flatMap { owner, _ -> Observable<Mutation> in
                return owner.fetchDataSource()
            }
            .catch { error in
                let error = error.asWMError
                return .concat(
                    .just(.undoDataSource),
                    .just(.showToast(error.errorDescription ?? "알 수 없는 오류가 발생하였습니다."))
                )
            }
    }

    func mutateEditSongsOrderUseCase() -> Observable<Mutation> {
        let currentDataSource = currentState.dataSource
        let songsOrder = currentDataSource.flatMap { $0.items.map { $0.song.id } }
        return editFavoriteSongsOrderUseCase.execute(ids: songsOrder)
            .asObservable()
            .flatMap { _ -> Observable<Mutation> in
                return .concat(
                    .just(.updateBackupDataSource(currentDataSource))
                )
            }
            .catch { error in
                let error = error.asWMError
                return Observable.concat([
                    .just(.undoDataSource),
                    .just(.showToast(error.errorDescription ?? "알 수 없는 오류가 발생하였습니다."))
                ])
            }
    }
}
