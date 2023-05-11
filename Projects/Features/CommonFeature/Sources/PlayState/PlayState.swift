//
//  PlayState.swift
//  PlayerFeature
//
//  Created by YoungK on 2023/02/20.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import DomainModule
import YouTubePlayerKit
import Combine
import Utility

final public class PlayState {
    public static let shared = PlayState()
    
    @Published public var player: YouTubePlayer
    @Published public var state: YouTubePlayer.PlaybackState
    @Published public var currentSong: SongEntity?
    @Published public var progress: PlayProgress
    @Published public var playList: PlayList
    @Published public var repeatMode: RepeatMode
    @Published public var shuffleMode: ShuffleMode
    
    private var subscription = Set<AnyCancellable>()
    
    init() {
        playList = PlayList()
        progress = PlayProgress()
        state = .unstarted
        repeatMode = .none
        shuffleMode = .off
        player = YouTubePlayer(configuration: .init(autoPlay: false, showControls: false, showRelatedVideos: false))
        
        playList.list = fetchPlayListFromLocalDB()
        currentSong = playList.currentPlaySong
        player.cue(source: .video(id: currentSong?.id ?? "")) // 곡이 있으면 .cued 없으면 .unstarted
        
        player.playbackStatePublisher.sink { [weak self] state in
            guard let self = self else { return }
            self.state = state
        }.store(in: &subscription)
        
        player.currentTimePublisher().sink { [weak self] currentTime in
            guard let self = self else { return }
            self.progress.currentProgress = currentTime
        }.store(in: &subscription)
        
        player.durationPublisher.sink { [weak self] duration in
            guard let self = self else { return }
            self.progress.endProgress = duration
        }.store(in: &subscription)
        
        Publishers.Merge4(
            playList.listAppended.dropFirst(),
            playList.listRemoved.dropFirst(),
            playList.listReordered.dropFirst(),
            playList.currentSongChanged.dropFirst()
        )
        .sink { playListItems in
            let allPlayedLists = RealmManager.shared.realm.objects(PlayedLists.self)
            RealmManager.shared.deleteRealmDB(model: allPlayedLists)
            
            let playedList = playListItems.map {
                PlayedLists(
                    id: $0.item.id,
                    title: $0.item.title,
                    artist: $0.item.artist,
                    remix: $0.item.remix,
                    reaction: $0.item.reaction,
                    views: $0.item.views,
                    last: $0.item.last,
                    date: $0.item.date,
                    lastPlayed: $0.isPlaying
                )}
            RealmManager.shared.addRealmDB(model: playedList)
        }.store(in: &subscription)
        
    }
    
    func fetchPlayListFromLocalDB() -> [PlayListItem] {
        let playedList = RealmManager.shared.realm.objects(PlayedLists.self)
            .toArray(type: PlayedLists.self)
            .map { PlayListItem(item:
                SongEntity(
                    id: $0.id,
                    title: $0.title,
                    artist: $0.artist,
                    remix: $0.remix,
                    reaction: $0.reaction,
                    views: $0.views,
                    last: $0.last,
                    date: $0.date),
                    isPlaying: $0.lastPlayed
            )}
        return playedList
    }
    
}
