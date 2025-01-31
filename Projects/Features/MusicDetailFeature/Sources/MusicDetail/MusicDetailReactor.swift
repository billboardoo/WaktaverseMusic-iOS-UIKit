import ArtistDomainInterface
import BaseFeature
import Foundation
import Kingfisher
import LikeDomainInterface
import Localization
import LogManager
import LyricHighlightingFeatureInterface
import ReactorKit
import RxSwift
import SongsDomainInterface
import Utility

final class MusicDetailReactor: Reactor {
    private typealias Log = MusicDetailAnalyticsLog
    private struct LikeRequest {
        let songID: String
        let isLiked: Bool
    }

    enum Action {
        case viewDidLoad
        case viewWillDisappear
        case prevButtonDidTap
        case playButtonDidTap
        case nextButtonDidTap
        case singingRoomButtonDidTap
        case lyricsButtonDidTap
        case creditButtonDidTap
        case likeButtonDidTap
        case musicPickButtonDidTap
        case playListButtonDidTap
        case dismissButtonDidTap
        case didTapArtistLabel
    }

    enum Mutation {
        case updateSelectedIndex(Int)
        case navigate(NavigateType?)
        case updateSongDictionary(key: String, value: SongModel)
    }

    enum NavigateType {
        case youtube(id: String, playPlatform: WakmusicYoutubePlayer.PlayPlatform)
        case credit(id: String)
        case lyricsHighlighting(model: LyricHighlightingRequiredModel)
        case musicPick(id: String)
        case playlist(id: String)
        case dismiss
        case textPopup(text: String, completion: () -> Void)
        case signin
        case karaoke(ky: Int?, tj: Int?)
        case artist(artistID: String)
    }

    struct State {
        var songIDs: [String]
        var selectedIndex: Int
        var songDictionary: [String: SongModel] = [:]
        var selectedSong: SongModel? {
            guard selectedIndex >= 0, selectedIndex < songIDs.count else { return nil }
            return songDictionary[songIDs[selectedIndex]]
        }

        var isFirstSong: Bool { selectedIndex == 0 }
        var isLastSong: Bool { selectedIndex == songIDs.count - 1 }
        var navigateType: NavigateType?
    }

    private let signInIsRequiredSubject = PublishSubject<Void>()

    var initialState: State
    private let youtubeURLGenerator = YoutubeURLGenerator()
    private let fetchSongUseCase: any FetchSongUseCase
    private let addLikeSongUseCase: any AddLikeSongUseCase
    private let cancelLikeSongUseCase: any CancelLikeSongUseCase
    private let findArtistIDUseCase: any FindArtistIDUseCase
    private var shouldRefreshLikeList = false

    private var pendingLikeRequests: [String: LikeRequest] = [:]
    private var latestLikeRequests: [String: LikeRequest] = [:]
    private let likeRequestSubject = PublishSubject<LikeRequest>()
    private var currentLikeRequestSongID: String?

    init(
        songIDs: [String],
        selectedID: String,
        fetchSongUseCase: any FetchSongUseCase,
        addLikeSongUseCase: any AddLikeSongUseCase,
        cancelLikeSongUseCase: any CancelLikeSongUseCase,
        findArtistIDUseCase: any FindArtistIDUseCase
    ) {
        let uniqueedSongIDs = Array(songIDs.uniqued())
        let selectedIndex = Array(uniqueedSongIDs).firstIndex(of: selectedID) ?? 0
        self.initialState = .init(
            songIDs: uniqueedSongIDs,
            selectedIndex: selectedIndex
        )

        self.fetchSongUseCase = fetchSongUseCase
        self.addLikeSongUseCase = addLikeSongUseCase
        self.cancelLikeSongUseCase = cancelLikeSongUseCase
        self.findArtistIDUseCase = findArtistIDUseCase

        let urls = [
            uniqueedSongIDs[safe: selectedIndex - 1],
            uniqueedSongIDs[safe: selectedIndex],
            uniqueedSongIDs[safe: selectedIndex + 1]
        ]
        .compactMap { $0 }
        .map { youtubeURLGenerator.generateHDThumbnailURL(id: $0) }
        .compactMap { URL(string: $0) }

        ImagePrefetcher(urls: urls).start()
    }

    func mutate(action: Action) -> Observable<Mutation> {
        switch action {
        case .viewDidLoad:
            return viewDidLoad()
        case .viewWillDisappear:
            return viewWillDisappear()
        case .prevButtonDidTap:
            return prevButtonDidTap()
        case .playButtonDidTap:
            return playButtonDidTap()
        case .nextButtonDidTap:
            return nextButtonDidTap()
        case .singingRoomButtonDidTap:
            return singingRoomButtonDiTap()
        case .lyricsButtonDidTap:
            return lyricsButtonDidTap()
        case .creditButtonDidTap:
            return creditButtonDidTap()
        case .likeButtonDidTap:
            return likeButtonDidTap()
        case .musicPickButtonDidTap:
            return musicPickButtonDidTap()
        case .playListButtonDidTap:
            return playListButtonDidTap()
        case .dismissButtonDidTap:
            return navigateMutation(navigate: .dismiss)
        case .didTapArtistLabel:
            return didTapArtistLabel()
        }
    }

    func reduce(state: State, mutation: Mutation) -> State {
        var newState = state
        switch mutation {
        case let .updateSelectedIndex(index):
            newState.selectedIndex = index
        case let .navigate(navigate):
            newState.navigateType = navigate
        case let .updateSongDictionary(key, value):
            newState.songDictionary[key] = value
        }
        return newState
    }

    func transform(mutation: Observable<Mutation>) -> Observable<Mutation> {
        let signInIsRequired = signInIsRequiredSubject
            .withUnretained(self)
            .flatMap { owner, _ in
                owner.navigateMutation(navigate: .signin)
            }

        let likeRequestObservable = likeRequestSubject
            .flatMapLatest { [weak self] request -> Observable<Mutation> in
                guard let self = self else { return .empty() }
                return self.handleLikeRequest(request)
            }

        return Observable.merge(mutation, signInIsRequired, likeRequestObservable)
    }

    /// 좋아요 요청을 처리하고 대기 중인 요청이 있다면 연속적으로 처리해요.
    ///
    /// 이 함수는 주어진 좋아요 요청을 처리하고, 해당 요청의 처리가 완료된 후 대기 중인 다른 요청이 있는지 확인해요.
    /// 대기 중인 요청이 있다면 해당 요청도 처리합니다.
    ///
    /// - Parameters:
    ///   - request: 처리할 좋아요 요청
    ///
    /// - Returns: 처리 결과에 따른 Mutation Observable 스트림
    ///
    /// - Note: 이 함수는 에러 발생 시 로그를 출력하고 빈 Observable을 반환해요.
    private func handleLikeRequest(_ request: LikeRequest) -> Observable<Mutation> {
        let useCase: (_ id: String) -> Observable<Void> = request.isLiked
            ? { [addLikeSongUseCase] id in
                addLikeSongUseCase.execute(id: id)
                    .asObservable()
                    .map { _ in () }
            }
            : { [cancelLikeSongUseCase] id in
                cancelLikeSongUseCase.execute(id: id)
                    .asObservable()
                    .map { _ in () }
            }

        self.latestLikeRequests[request.songID] = request
        self.pendingLikeRequests.removeValue(forKey: request.songID)
        return useCase(request.songID)
            .asObservable()
            .flatMap { [weak self] _ -> Observable<Mutation> in
                guard let self = self else { return .empty() }

                if let nextRequest = self.getNextPendingRequest() {
                    return self.handleLikeRequest(nextRequest)
                }

                self.currentLikeRequestSongID = nil
                return .empty()
            }
            .catch { [weak self] error in
                self?.currentLikeRequestSongID = nil
                LogManager.printError("Like request failed: \(error)")
                return .empty()
            }
    }

    /// 다음으로 처리할 대기 중인 좋아요 요청을 반환해요.
    ///
    /// 이 함수는 현재 처리 중인 곡의 ID에 해당하는 대기 중인 요청이 있는지 먼저 확인해요.
    /// 만약 있다면, 해당 요청의 상태가 최신 상태와 다를 경우에만 반환해요.
    /// 현재 곡의 대기 중인 요청이 없거나 상태가 같다면, 다른 곡의 대기 중인 요청 중 첫 번째 요청을 반환해요.
    ///
    /// - Returns: 다음으로 처리할 `LikeRequest` 객체. 처리할 마땅한 요청이 없다면 `nil`을 반환
    ///
    /// - Note: 이 함수는 대기 중인 요청들의 우선순위에 따라 반환됩니다.
    private func getNextPendingRequest() -> LikeRequest? {
        if let currentSongID = currentLikeRequestSongID,
           let nextPendingRequest = pendingLikeRequests[currentSongID] {
            let latestRequest = latestLikeRequests[currentSongID]
            return if latestRequest == nil {
                nextPendingRequest
            } else if nextPendingRequest.isLiked != latestRequest?.isLiked {
                nextPendingRequest
            } else {
                nil
            }
        }

        guard let remainRequest = pendingLikeRequests.values.first else { return nil }

        return remainRequest
    }
}

// MARK: - Mutate

private extension MusicDetailReactor {
    func viewDidLoad() -> Observable<Mutation> {
        let selectedIndex = currentState.selectedIndex
        guard let selectedSongID = currentState.songIDs[safe: selectedIndex] else { return .empty() }
        let prefetchingSongMutationObservable = [
            currentState.songIDs[safe: selectedIndex - 1],
            currentState.songIDs[safe: selectedIndex + 1]
        ].compactMap { $0 }
            .map { index in
                fetchSongUseCase.execute(id: index)
                    .map { $0.toModel() }
                    .catchAndReturn(SongModel.notFoundModel())
                    .map { Mutation.updateSongDictionary(key: index, value: $0) }
                    .asObservable()
            }

        let songMutationObservable = fetchSongUseCase.execute(id: selectedSongID)
            .map { $0.toModel() }
            .catchAndReturn(SongModel.notFoundModel())
            .map { Mutation.updateSongDictionary(key: selectedSongID, value: $0) }
            .asObservable()

        return Observable.merge(
            [songMutationObservable] + prefetchingSongMutationObservable
        )
    }

    func viewWillDisappear() -> Observable<Mutation> {
        if shouldRefreshLikeList {
            NotificationCenter.default.post(name: .shouldRefreshLikeList, object: nil)
        }
        return .empty()
    }

    func prevButtonDidTap() -> Observable<Mutation> {
        guard !currentState.isFirstSong else { return .empty() }
        if let song = currentState.selectedSong {
            let log = Log.clickPrevMusicButton(id: song.videoID)
            LogManager.analytics(log)
        }
        let newIndex = currentState.selectedIndex - 1
        prefetchThumbnailImage(index: newIndex)

        return .concat(
            .just(.updateSelectedIndex(newIndex)),
            fetchSongDetailWith(index: newIndex)
        )
    }

    func playButtonDidTap() -> Observable<Mutation> {
        guard let song = currentState.selectedSong, !song.videoID.isEmpty else { return .empty() }
        if let song = currentState.selectedSong {
            let log = CommonAnalyticsLog.clickPlayButton(location: .musicDetail, type: .single)
            LogManager.analytics(log)
        }
        PlayState.shared.append(item: PlaylistItem(id: song.videoID, title: song.title, artist: song.artistString))
        return navigateMutation(navigate: .youtube(
            id: song.videoID,
            playPlatform: song.title.isContainShortsTagTitle ? .youtube : .automatic
        ))
    }

    func nextButtonDidTap() -> Observable<Mutation> {
        guard !currentState.isLastSong else { return .empty() }
        if let song = currentState.selectedSong {
            let log = Log.clickNextMusicButton(
                id: song.videoID
            )
            LogManager.analytics(log)
        }
        let newIndex = currentState.selectedIndex + 1
        prefetchThumbnailImage(index: newIndex)

        return .concat(
            .just(.updateSelectedIndex(newIndex)),
            fetchSongDetailWith(index: newIndex)
        )
    }

    func singingRoomButtonDiTap() -> Observable<Mutation> {
        guard let song = currentState.selectedSong, !song.videoID.isEmpty else { return .empty() }
        let log = Log.clickSingingRoomButton(id: song.videoID)
        LogManager.analytics(log)
        return navigateMutation(navigate: .karaoke(ky: song.karaokeNumber.ky, tj: song.karaokeNumber.tj))
    }

    func lyricsButtonDidTap() -> Observable<Mutation> {
        guard let song = currentState.selectedSong, !song.videoID.isEmpty else { return .empty() }
        let log = Log.clickLyricsButton(id: song.videoID)
        LogManager.analytics(log)

        let lyricHighlightingModel = LyricHighlightingRequiredModel(
            songID: song.videoID,
            title: song.title,
            artist: song.artistString
        )
        return navigateMutation(navigate: .lyricsHighlighting(model: lyricHighlightingModel))
    }

    func creditButtonDidTap() -> Observable<Mutation> {
        guard let song = currentState.selectedSong, !song.videoID.isEmpty else { return .empty() }
        let log = Log.clickCreditButton(id: song.videoID)
        LogManager.analytics(log)
        return navigateMutation(navigate: .credit(id: song.videoID))
    }

    func likeButtonDidTap() -> Observable<Mutation> {
        guard PreferenceManager.shared.userInfo != nil else {
            return navigateMutation(
                navigate: NavigateType.textPopup(
                    text: LocalizationStrings.needLoginWarning,
                    completion: { [signInIsRequiredSubject] in
                        let log = CommonAnalyticsLog.clickLoginButton(entry: .songLike)
                        LogManager.analytics(log)

                        signInIsRequiredSubject.onNext(())
                    }
                )
            )
        }

        guard let song = currentState.selectedSong, !song.videoID.isEmpty else { return .empty() }
        let isLike = currentState.selectedSong?.isLiked ?? false
        let log = Log.clickLikeMusicButton(
            id: song.videoID,
            like: isLike
        )

        let newLike = !isLike
        let newSong = if newLike {
            song.updateIsLiked(likes: song.likes + 1, isLiked: newLike)
        } else {
            song.updateIsLiked(likes: song.likes - 1, isLiked: newLike)
        }
        LogManager.analytics(log)

        let request = LikeRequest(songID: song.videoID, isLiked: newLike)
        pendingLikeRequests[song.videoID] = request

        let currentLikeRequestSongID = currentLikeRequestSongID
        self.currentLikeRequestSongID = song.videoID
        if currentLikeRequestSongID == nil {
            likeRequestSubject.onNext(request)
        }

        shouldRefreshLikeList = true
        return .just(.updateSongDictionary(key: newSong.videoID, value: newSong))
    }

    func musicPickButtonDidTap() -> Observable<Mutation> {
        let log = CommonAnalyticsLog.clickAddMusicsButton(location: .songDetail)
        LogManager.analytics(log)

        guard PreferenceManager.shared.userInfo != nil else {
            return navigateMutation(
                navigate: NavigateType.textPopup(
                    text: LocalizationStrings.needLoginWarning,
                    completion: { [signInIsRequiredSubject] in
                        let log = CommonAnalyticsLog.clickLoginButton(entry: .addMusics)
                        LogManager.analytics(log)

                        signInIsRequiredSubject.onNext(())
                    }
                )
            )
        }
        guard let song = currentState.selectedSong, !song.videoID.isEmpty else { return .empty() }
        return navigateMutation(navigate: .musicPick(id: song.videoID))
    }

    func playListButtonDidTap() -> Observable<Mutation> {
        guard let song = currentState.selectedSong else { return .empty() }
        let log = Log.clickPlaylistButton(id: song.videoID)
        LogManager.analytics(log)
        return navigateMutation(navigate: .playlist(id: song.videoID))
    }

    func didTapArtistLabel() -> Observable<Mutation> {
        guard let selectedSong = currentState.selectedSong else { return .empty() }
        let artists = selectedSong.artistString.components(separatedBy: ",")

        if artists.count == 1, let artistName = artists.first {
            return findArtistIDUseCase.execute(name: artistName)
                .asObservable()
                .flatMap {
                    return Observable.concat(
                        .just(Mutation.navigate(.artist(artistID: $0))),
                        .just(.navigate(nil))
                    )
                }
                .catchAndReturn(.navigate(.credit(id: selectedSong.videoID)))
        } else {
            return navigateMutation(navigate: .credit(id: selectedSong.videoID))
        }
    }
}

// MARK: - Private Methods

private extension MusicDetailReactor {
    func navigateMutation(navigate: NavigateType) -> Observable<Mutation> {
        return .concat(
            .just(.navigate(navigate)),
            .just(.navigate(nil))
        )
    }

    func prefetchThumbnailImage(index: Int) {
        let prefetchingSongImageURLs = [
            currentState.songIDs[safe: index - 1],
            currentState.songIDs[safe: index],
            currentState.songIDs[safe: index + 1]
        ].compactMap { $0 }
            .compactMap { URL(string: youtubeURLGenerator.generateHDThumbnailURL(id: $0)) }
        ImagePrefetcher(urls: prefetchingSongImageURLs).start()
    }

    func fetchSongDetailWith(index: Int) -> Observable<Mutation> {
        let prefetchingSongMutationObservable = [
            currentState.songIDs[safe: index - 1],
            currentState.songIDs[safe: index + 1]
        ].compactMap { $0 }
            .filter { currentState.songDictionary[$0] == nil }
            .map { index in
                fetchSongUseCase.execute(id: index)
                    .map { $0.toModel() }
                    .catchAndReturn(SongModel.notFoundModel())
                    .map { Mutation.updateSongDictionary(key: index, value: $0) }
                    .asObservable()
            }

        guard let songID = currentState.songIDs[safe: index] else {
            return Observable.merge(
                prefetchingSongMutationObservable
            )
        }

        guard currentState.songDictionary[songID] == nil else {
            return Observable.merge(
                prefetchingSongMutationObservable
            )
        }

        let currentSongMutationObservable = fetchSongUseCase.execute(id: songID)
            .map { $0.toModel() }
            .catchAndReturn(SongModel.notFoundModel())
            .map { Mutation.updateSongDictionary(key: songID, value: $0) }
            .asObservable()

        return Observable.merge(
            [currentSongMutationObservable] + prefetchingSongMutationObservable
        )
    }
}

private extension SongModel {
    static func notFoundModel() -> SongModel {
        .init(
            videoID: "",
            title: "해당 곡을 찾을 수 없습니다",
            artistString: "정보를 가져올 수 없습니다",
            date: "",
            views: 0,
            likes: 0,
            isLiked: false,
            karaokeNumber: .init(tj: nil, ky: nil)
        )
    }
}
