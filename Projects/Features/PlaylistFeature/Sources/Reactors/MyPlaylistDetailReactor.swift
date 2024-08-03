import AuthDomainInterface
import Foundation
import PlaylistDomainInterface
import ReactorKit
import RxSwift
import SongsDomainInterface
import Utility

final class MyPlaylistDetailReactor: Reactor {
    enum Action {
        case viewDidLoad
        case itemDidTap(Int)
        case editButtonDidTap
        case privateButtonDidTap
        case completeButtonDidTap
        case restore
        case itemDidMoved(Int, Int)
        case forceSave
        case forceEndEditing
        case changeTitle(String)
        case selectAll
        case deselectAll
        case removeSongs
        case changeImageData(PlaylistImageKind)
        case shareButtonDidTap
    }

    enum Mutation {
        case updateEditingState(Bool)
        case updateHeader(PlaylistDetailHeaderModel)
        case updatePlaylist([PlaylistItemModel])
        case updateBackUpPlaylist([PlaylistItemModel])
        case updateLoadingState(Bool)
        case updateSelectedCount(Int)
        case updateImageData(PlaylistImageKind?)
        case showToast(String)
        case showShareLink(String)
        case updateRefresh
    }

    struct State {
        var isEditing: Bool
        var header: PlaylistDetailHeaderModel
        var playlistModels: [PlaylistItemModel]
        var backupPlaylistModels: [PlaylistItemModel]
        var isLoading: Bool
        var selectedCount: Int
        var imageData: PlaylistImageKind?
        @Pulse var toastMessage: String?
        @Pulse var shareLink: String?
        @Pulse var refresh: Void?
    }

    internal let key: String
    var initialState: State
    private let fetchPlaylistDetailUseCase: any FetchPlaylistDetailUseCase
    private let updatePlaylistUseCase: any UpdatePlaylistUseCase
    private let updateTitleAndPrivateUseCase: any UpdateTitleAndPrivateUseCase
    private let removeSongsUseCase: any RemoveSongsUseCase
    private let uploadDefaultPlaylistImageUseCase: any UploadDefaultPlaylistImageUseCase
    private let requestCustomImageURLUseCase: any RequestCustomImageURLUseCase

    private let logoutUseCase: any LogoutUseCase
    private let deepLinkGenerator = WMDeepLinkGenerator()

    init(
        key: String,
        fetchPlaylistDetailUseCase: any FetchPlaylistDetailUseCase,
        updatePlaylistUseCase: any UpdatePlaylistUseCase,
        updateTitleAndPrivateUseCase: any UpdateTitleAndPrivateUseCase,
        removeSongsUseCase: any RemoveSongsUseCase,
        uploadDefaultPlaylistImageUseCase: any UploadDefaultPlaylistImageUseCase,
        requestCustomImageURLUseCase: any RequestCustomImageURLUseCase,
        logoutUseCase: any LogoutUseCase

    ) {
        self.key = key
        self.fetchPlaylistDetailUseCase = fetchPlaylistDetailUseCase
        self.updatePlaylistUseCase = updatePlaylistUseCase
        self.updateTitleAndPrivateUseCase = updateTitleAndPrivateUseCase
        self.removeSongsUseCase = removeSongsUseCase
        self.uploadDefaultPlaylistImageUseCase = uploadDefaultPlaylistImageUseCase
        self.requestCustomImageURLUseCase = requestCustomImageURLUseCase
        self.logoutUseCase = logoutUseCase

        self.initialState = State(
            isEditing: false,
            header: PlaylistDetailHeaderModel(
                key: key,
                title: "",
                image: "",
                userName: "",
                private: true,
                songCount: 0
            ),
            playlistModels: [],
            backupPlaylistModels: [],
            isLoading: true,
            selectedCount: 0,
            refresh: nil
        )
    }

    func mutate(action: Action) -> Observable<Mutation> {
        switch action {
        case .viewDidLoad:
            return viewDidLoad()

        case .editButtonDidTap:
            return beginEditing()

        case .privateButtonDidTap:
            return updatePrivate()

        case .forceSave, .completeButtonDidTap:
            return endEditingWithSave()

        case .forceEndEditing:
            return endEditing()

        case let .itemDidTap(index):
            return updateItemSelected(index)

        case .restore:
            return restoreDataSource()
        case let .itemDidMoved(from, to):
            return updateItemPosition(from: from, to: to)

        case let .changeTitle(text):
            return updateTitle(text: text)

        case .selectAll:
            return selectAll()

        case .deselectAll:
            return deselectAll()

        case .removeSongs:
            return removeSongs()

        case let .changeImageData(imageData):
            return updateImageData(imageData: imageData)
        case .shareButtonDidTap:
            return .just(.showShareLink(deepLinkGenerator.generatePlaylistDeepLink(key: key)))
        }
    }

    func reduce(state: State, mutation: Mutation) -> State {
        var newState = state

        switch mutation {
        case let .updateEditingState(flag):
            newState.isEditing = flag

        case let .updateHeader(header):
            newState.header = header

        case let .updatePlaylist(platylist):
            newState.playlistModels = platylist

        case let .updateBackUpPlaylist(dataSource):
            newState.backupPlaylistModels = dataSource

        case let .updateLoadingState(isLoading):
            newState.isLoading = isLoading

        case let .updateSelectedCount(count):
            newState.selectedCount = count

        case let .showToast(message):
            newState.toastMessage = message
        case let .updateImageData(imageData):
            newState.imageData = imageData

        case let .showShareLink(link):
            newState.shareLink = link
        case .updateRefresh:
            newState.refresh = ()
        }

        return newState
    }
}

private extension MyPlaylistDetailReactor {
    func viewDidLoad() -> Observable<Mutation> {
        return .concat([
            .just(.updateLoadingState(true)),
            fetchPlaylistDetailUseCase.execute(id: key, type: .my)
                .asObservable()
                .flatMap { data -> Observable<Mutation> in
                    return .concat([
                        Observable.just(Mutation.updateHeader(
                            PlaylistDetailHeaderModel(
                                key: data.key,
                                title: data.title,
                                image: data.image,
                                userName: data.userName,
                                private: data.private,
                                songCount: data.songs.count
                            )
                        )),
                        Observable.just(
                            Mutation.updatePlaylist(
                                data.songs.map {
                                    PlaylistItemModel(
                                        id: $0.id,
                                        title: $0.title,
                                        artist: $0.artist,
                                        isSelected: false
                                    )
                                }
                            )
                        ),
                        Observable.just(
                            Mutation.updateBackUpPlaylist(
                                data.songs.map {
                                    PlaylistItemModel(
                                        id: $0.id,
                                        title: $0.title,
                                        artist: $0.artist,
                                        isSelected: false
                                    )
                                }
                            )
                        )
                    ])
                }
                .catch { error in
                    let wmErorr = error.asWMError
                    return Observable.just(
                        Mutation.showToast(wmErorr.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                    )
                },
            .just(.updateLoadingState(false))
        ])
    }

    func endEditingWithSave() -> Observable<Mutation> {
        let state = currentState
        let currentPlaylists = state.playlistModels

        let newPlaylistItemModels = currentPlaylists.map {
            return $0.updateIsSelected(isSelected: false)
        }

        var mutations: [Observable<Mutation>] = []

        if let imageData = state.imageData {
            switch imageData {
            case let .default(imageName):
                mutations.append(
                    uploadDefaultPlaylistImageUseCase.execute(key: self.key, model: imageName)
                        .andThen(.empty())
                        .catch { error in
                            let wmErorr = error.asWMError
                            return Observable.just(
                                Mutation.showToast(wmErorr.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                            )
                        }
                )

            case let .custom(data):
                mutations.append(
                    requestCustomImageURLUseCase.execute(key: self.key, data: data)
                        .andThen(.empty())
                        .catch { error in
                            let wmErorr = error.asWMError
                            return Observable.just(
                                Mutation.showToast(wmErorr.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                            )
                        }
                )
            }
        }
        if state.backupPlaylistModels == newPlaylistItemModels {
            mutations.append(endEditing())
        } else {
            mutations.append(
                updatePlaylistUseCase.execute(key: key, songs: currentPlaylists.map { $0.id })
                    .andThen(endEditing())
                    .catch { error in
                        let wmErorr = error.asWMError
                        return Observable.just(
                            Mutation.showToast(wmErorr.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                        )
                    }
            )
        }

        return .concat(mutations)
    }

    func endEditing() -> Observable<Mutation> {
        let state = currentState
        let currentPlaylists = state.playlistModels

        let updatingPlaylistItemModels = currentPlaylists.map {
            return $0.updateIsSelected(isSelected: false)
        }

        return .concat([
            .just(.updateEditingState(false)),
            .just(.updatePlaylist(updatingPlaylistItemModels)),
            .just(.updateBackUpPlaylist(updatingPlaylistItemModels)),
            .just(.updateSelectedCount(0)),
            .just(.updateImageData(nil))
        ])
    }

    func updatePrivate() -> Observable<Mutation> {
        let state = currentState

        var prev = state.header
        prev.updatePrivate()

        let message: String = prev.private ? "리스트를 비공개 처리했습니다." : "리스트를 공개 처리했습니다."

        return updateTitleAndPrivateUseCase.execute(key: key, title: nil, isPrivate: prev.private)
            .andThen(.concat([
                .just(.updateHeader(prev)),
                .just(.showToast(message))

            ]))
            .catch { error in
                let wmErorr = error.asWMError
                return Observable.just(
                    Mutation.showToast(wmErorr.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                )
            }
    }

    func updateTitle(text: String) -> Observable<Mutation> {
        let state = currentState

        var prev = state.header
        prev.updateTitle(text)

        return .concat([
            .just(.updateHeader(prev)),
            updateTitleAndPrivateUseCase.execute(key: key, title: text, isPrivate: nil)
                .andThen(updateSendRefreshNoti())
        ])
    }
}

/// usecase를 사용하지 않는
private extension MyPlaylistDetailReactor {
    func beginEditing() -> Observable<Mutation> {
        let state = currentState
        let currentPlaylists = state.playlistModels

        return .concat([
            .just(.updateEditingState(true)),
            .just(.updateBackUpPlaylist(currentPlaylists))
        ])
    }

    func updateItemSelected(_ index: Int) -> Observable<Mutation> {
        let state = currentState
        var count = state.selectedCount
        // 불변성 고려
        var playlists = state.playlistModels
        let isSelected = playlists[index].isSelected

        if playlists[index].isSelected {
            count -= 1
        } else {
            count += 1
        }
        playlists[index] = playlists[index].updateIsSelected(isSelected: !isSelected)

        return .concat([
            .just(Mutation.updateSelectedCount(count)),
            .just(Mutation.updatePlaylist(playlists))
        ])
    }

    func updateItemPosition(from: Int, to: Int) -> Observable<Mutation> {
        let state = currentState
        var updatingPlaylists = state.playlistModels

        let item = updatingPlaylists[from]

        updatingPlaylists.remove(at: from)

        updatingPlaylists.insert(item, at: to)

        return .just(Mutation.updatePlaylist(updatingPlaylists))
    }

    func restoreDataSource() -> Observable<Mutation> {
        let state = currentState
        let backUpPlaylist = state.backupPlaylistModels

        return .concat([
            .just(Mutation.updateEditingState(false)),
            .just(Mutation.updatePlaylist(backUpPlaylist)),
            .just(.updateSelectedCount(0))
        ])
    }

    func selectAll() -> Observable<Mutation> {
        let state = currentState
        var playlist = state.playlistModels

        let updatingPlaylistItemModels = playlist.map {
            return $0.updateIsSelected(isSelected: true)
        }

        return .concat([
            .just(.updatePlaylist(updatingPlaylistItemModels)),
            .just(.updateSelectedCount(updatingPlaylistItemModels.count))
        ])
    }

    func deselectAll() -> Observable<Mutation> {
        let state = currentState
        var playlist = state.playlistModels

        let updatingPlaylistItemModels = playlist.map {
            return $0.updateIsSelected(isSelected: false)
        }

        return .concat([
            .just(.updatePlaylist(updatingPlaylistItemModels)),
            .just(.updateSelectedCount(0)),
            .just(.updateImageData(nil))
        ])
    }

    func removeSongs() -> Observable<Mutation> {
        let state = currentState
        let playlists = state.playlistModels

        let remainSongs = playlists.filter { !$0.isSelected }
        let removeSongs = playlists.filter { $0.isSelected }.map { $0.id }
        var prevHeader = currentState.header
        prevHeader.updateSongCount(remainSongs.count)

        return removeSongsUseCase.execute(key: key, songs: removeSongs)
            .andThen(.concat([
                .just(.updatePlaylist(remainSongs)),
                .just(.updateBackUpPlaylist(remainSongs)),
                .just(.updateEditingState(false)),
                .just(.updateSelectedCount(0)),
                .just(.updateHeader(prevHeader)),
                .just(.showToast("\(remainSongs.count)개의 곡을 삭제했습니다.")),
                updateSendRefreshNoti()

            ]))
            .catch { error in
                let wmErorr = error.asWMError
                return Observable.just(
                    Mutation.showToast(wmErorr.errorDescription ?? "알 수 없는 오류가 발생하였습니다.")
                )
            }
    }

    func updateImageData(imageData: PlaylistImageKind?) -> Observable<Mutation> {
        return .just(.updateImageData(imageData))
    }

    func updateSendRefreshNoti() -> Observable<Mutation> {
        .just(.updateRefresh)
    }
}
