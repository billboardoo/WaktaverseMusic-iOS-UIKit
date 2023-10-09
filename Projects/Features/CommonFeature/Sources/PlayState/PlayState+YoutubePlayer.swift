//
//  PlayState+YoutubePlayer.swift
//  CommonFeature
//
//  Created by YoungK on 2023/04/01.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import DomainModule
import Utility

// MARK: YouTubePlayer 컨트롤과 관련된 메소드들을 모아놓은 익스텐션입니다.
extension PlayState {
    
    /// ⏯️ 현재 곡 재생
    public func play() {
        self.player?.play()
    }
    
    /// ⏸️ 일시정지
    public func pause() {
        self.player?.pause()
    }
    
    /// ⏹️ 플레이어 닫기
    public func stop() {
        self.player?.stop() // stop만 하면 playbackState가 .cued로 들어감
        self.currentSong = nil
        self.progress.resetCurrentProgress()
        self.player?.cue(source: .video(id: "")) // playbackState를 .unstarted로 바꿈
        //self.playList.removeAll()
    }
    
    /// ▶️ 해당 곡 새로 재생
    public func load(at song: SongEntity) {
        //requestPlaybackLog(current: song) // v2 api 완성 후 적용하기로 함.
        self.currentSong = song
        guard let currentSong = currentSong else { return }
        self.player?.load(source: .video(id: currentSong.id))
    }
    
    /// ▶️ 플레이리스트의 해당 위치의  곡 재생
    public func loadInPlaylist(at index: Int) {
        guard let playListItem = playList.list[safe: index] else { return }
        load(at: playListItem.item)
    }
    
    /// ⏩ 다음 곡으로 변경 후 재생
    public func forward() {
        self.playList.changeCurrentPlayIndexToNext()
        guard let currentPlayIndex = playList.currentPlayIndex else { return }
        guard let playListItem = playList.list[safe: currentPlayIndex] else { return }
        load(at: playListItem.item)
    }
    
    /// ⏪ 이전 곡으로 변경 후 재생
    public func backward() {
        self.playList.changeCurrentPlayIndexToPrevious()
        guard let currentPlayIndex = playList.currentPlayIndex else { return }
        guard let playListItem = playList.list[safe: currentPlayIndex] else { return }
        load(at: playListItem.item)
    }
    
    /// 🔀 플레이리스트 내 랜덤 재생
    public func shufflePlay() {
        let shuffledIndices = self.playList.list.indices.shuffled()
        if let index = shuffledIndices.first(where: { $0 != self.playList.currentPlayIndex }) {
            self.playList.changeCurrentPlayIndex(to: index)
            self.loadInPlaylist(at: index)
        } else {
            self.forward()
        }
    }
    
    /// ♻️ 첫번째 곡으로 변경 후 재생
    public func playAgain() {
        self.playList.changeCurrentPlayIndex(to: 0)
        guard let firstItem = self.playList.first else { return }
        load(at: firstItem)
    }
    
    /// 🎤 재생 로그 전송
    public func requestPlaybackLog(current: SongEntity) {
        guard Utility.PreferenceManager.userInfo != nil else {
            return
        }
        let logItem = PlayState.PlaybackLog(prev: PlayState.PlaybackLog.Previous(songId: self.currentSong?.id ?? "",
                                                                                 songLength: Int(self.progress.endProgress),
                                                                                 stoppedAt: Int(self.progress.currentProgress)),
                                            curr: PlayState.PlaybackLog.Current(songId: current.id))
        NotificationCenter.default.post(name: .requestPlaybackLog, object: logItem)
    }
}
