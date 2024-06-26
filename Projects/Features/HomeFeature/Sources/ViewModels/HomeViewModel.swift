//
//  HomeViewModel.swift
//  HomeFeature
//
//  Created by KTH on 2023/02/20.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import BaseFeature
import ChartDomainInterface
import Foundation
import LogManager
import PlaylistDomainInterface
import RxCocoa
import RxSwift
import SongsDomainInterface
import Utility

typealias HomeUseCase = Observable<([ChartRankingEntity], [NewSongsEntity], [RecommendPlaylistEntity])>

public final class HomeViewModel: ViewModelType {
    private let disposeBag = DisposeBag()
    var fetchChartRankingUseCase: FetchChartRankingUseCase
    var fetchNewSongsUseCase: FetchNewSongsUseCase
    var fetchRecommendPlaylistUseCase: FetchRecommendPlayListUseCase

    public init(
        fetchChartRankingUseCase: any FetchChartRankingUseCase,
        fetchNewSongsUseCase: any FetchNewSongsUseCase,
        fetchRecommendPlaylistUseCase: any FetchRecommendPlayListUseCase
    ) {
        self.fetchChartRankingUseCase = fetchChartRankingUseCase
        self.fetchNewSongsUseCase = fetchNewSongsUseCase
        self.fetchRecommendPlaylistUseCase = fetchRecommendPlaylistUseCase
    }

    public struct Input {
        let fetchHomeUseCase: PublishSubject<Void> = PublishSubject()
        let chartAllListenTapped: PublishSubject<Void> = PublishSubject()
        let newSongsAllListenTapped: PublishSubject<Void> = PublishSubject()
        let refreshPulled: PublishSubject<Void> = PublishSubject()
    }

    public struct Output {
        let chartDataSource: BehaviorRelay<[ChartRankingEntity]> = BehaviorRelay(value: [])
        let newSongDataSource: BehaviorRelay<[NewSongsEntity]> = BehaviorRelay(value: [])
        let playListDataSource: BehaviorRelay<[RecommendPlaylistEntity]> = BehaviorRelay(value: [])
    }

    public func transform(from input: Input) -> Output {
        let output = Output()
        let chart = self.fetchChartRankingUseCase
            .execute(type: .hourly)
            .catchAndReturn(.init(updatedAt: "팬치들 미안해요 ㅠㅠ 잠시만 기다려주세요.", songs: []))
            .map { $0.songs }
            .asObservable()

        let newSongs = self.fetchNewSongsUseCase
            .execute(type: .all, page: 1, limit: 10)
            .catchAndReturn([])
            .asObservable()

        let playList = self.fetchRecommendPlaylistUseCase
            .execute()
            .catchAndReturn([])
            .asObservable()

        let homeSceneUseCase = Observable.zip(chart, newSongs, playList)

        input.fetchHomeUseCase
            .flatMap { _ -> HomeUseCase in
                return homeSceneUseCase
            }
            .subscribe(onNext: { arg in
                let (chartRankingEntity, newSongEntity, recommendPlayListEntity) = arg
                output.chartDataSource.accept(chartRankingEntity)
                output.newSongDataSource.accept(newSongEntity)
                output.playListDataSource.accept(recommendPlayListEntity)
            })
            .disposed(by: disposeBag)

        input.chartAllListenTapped
            .withLatestFrom(output.chartDataSource)
            .subscribe(onNext: { songs in
                let songEntities: [SongEntity] = songs.map {
                    return SongEntity(
                        id: $0.id,
                        title: $0.title,
                        artist: $0.artist,
                        remix: "",
                        reaction: "",
                        views: $0.views,
                        last: $0.last,
                        date: $0.date
                    )
                }
                LogManager.analytics(HomeAnalyticsLog.clickAllChartTop100MusicsButton)
                PlayState.shared.loadAndAppendSongsToPlaylist(songEntities)
            })
            .disposed(by: disposeBag)

        input.newSongsAllListenTapped
            .withLatestFrom(output.newSongDataSource)
            .subscribe(onNext: { newSongs in
                let songEntities: [SongEntity] = newSongs.map {
                    return SongEntity(
                        id: $0.id,
                        title: $0.title,
                        artist: $0.artist,
                        remix: "",
                        reaction: "",
                        views: $0.views,
                        last: 0,
                        date: "\($0.date)"
                    )
                }
                PlayState.shared.loadAndAppendSongsToPlaylist(songEntities)
            })
            .disposed(by: disposeBag)

        input.refreshPulled
            .flatMap { _ -> HomeUseCase in
                return homeSceneUseCase
            }
            .debug("✅ Refresh Completed")
            .subscribe(onNext: { arg in
                let (chartRankingEntity, newSongEntity, recommendPlayListEntity) = arg
                output.chartDataSource.accept(chartRankingEntity)
                output.newSongDataSource.accept(newSongEntity)
                output.playListDataSource.accept(recommendPlayListEntity)
            })
            .disposed(by: disposeBag)

        return output
    }
}
