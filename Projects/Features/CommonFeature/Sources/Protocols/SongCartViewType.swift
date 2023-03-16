//
//  SongCartViewType.swift
//  Utility
//
//  Created by KTH on 2023/03/15.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import UIKit
import Utility
import DesignSystem

public protocol SongCartViewType: AnyObject {
    var songCartView: SongCartView! { get set }
    var bottomSheetView: BottomSheetView! { get set }
}

public enum SongCartType {
    case playerToPlayList // 플레이어 > 재생목록
    case chartSong        // 차트
    case searchSong       // 검색
    case artistSong       // 아티스트
    case likeSong         // 보관함 > 좋아요
    case myPlayList       // 보관함 > 내 리스트
    case playList         // 플레이 리스트 (상세)
    case WMPlayList       // 추천 플레이 리스트 (상세)
}

public extension SongCartViewType where Self: UIViewController {
    
    /// 노래 담기 팝업을 띄웁니다.
    /// - Parameter view: 팝업을 붙일 대상이 되는 뷰 (ex: 아티스트 노래 리스트, viewController.view)
    /// - Parameter contentHeight: (변경할 일 있으면 사용)
    /// - Parameter backgroundColor: 백그라운드 컬러 (변경할 일 있으면 사용)
    /// - Parameter selectedSongCount: 현재 선택된 노래 갯수
    /// - Parameter totalSongCount: 전체 노래 갯수
    /// - Parameter useBottomSpace: 하단의 간격 사용 여부
    func showSongCart(
        in view: UIView,
        type: SongCartType,
        contentHeight: CGFloat = 56,
        backgroundColor: UIColor = UIColor.clear,
        selectedSongCount: Int,
        totalSongCount: Int,
        useBottomSpace: Bool = false
    ) {
        if self.songCartView == nil || self.bottomSheetView == nil {
            self.songCartView = SongCartView(type: type)
            self.bottomSheetView = BottomSheetView(
                contentView: self.songCartView,
                contentHeights: [contentHeight + (useBottomSpace ? SAFEAREA_BOTTOM_HEIGHT() : 0)]
            )
        }
        
        guard
            let songCartView = self.songCartView,
            let bottomSheetView = self.bottomSheetView
        else { return }
        
        // 하단 마진 사용 여부 업데이트
        songCartView.updateBottomSpace(isUse: useBottomSpace)
        
        // 선택된 노래 갯수 업데이트
        songCartView.updateCount(value: selectedSongCount)
        
        // 선택된 노래 갯수와 전체 노래 갯수를 비교하여 전체선택이 되었는지 비교하여 덥데이트
        songCartView.updateAllSelect(isAll: selectedSongCount == totalSongCount)

        // bottomSheetView가 해당 뷰에 붙지 않았을때만 present 합니다.
        guard
            !view.subviews.contains(self.bottomSheetView)
        else { return }
        
        bottomSheetView.present(in: view)
        
        // 메인 컨테이너 뷰컨에서 해당 노티를 수신, 팝업이 올라오면 미니 플레이어를 숨깁니다.
        NotificationCenter.default.post(name: .showSongCart, object: nil)
    }

    /// 노래 담기 팝업을 제거합니다.
    func hideSongCart() {
        guard
            let bottomSheetView = self.bottomSheetView
        else { return }
        bottomSheetView.dismiss()
        
        self.songCartView = nil
        self.bottomSheetView = nil
        
        // 메인 컨테이너 뷰컨에서 해당 노티를 수신, 팝업이 올라오면 미니 플레이어를 다시 보여줍니다.
        NotificationCenter.default.post(name: .hideSongCart, object: nil)
    }
}
