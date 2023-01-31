//
//  Extension+PreferenceManager.swift
//  Utility
//
//  Created by KTH on 2023/01/09.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import RxSwift

public extension PreferenceManager {
    
    /// 최근 검색어를 저장
    /// - Parameter word: 최근 검색어
    func addRecentRecords(word: String) {
        let maxSize: Int = 10
        var currentRecentRecords = Utility.PreferenceManager.recentRecords ?? []
        
        if currentRecentRecords.contains(word) {
            if let i = currentRecentRecords.firstIndex(where: { $0 == word }){
                currentRecentRecords.remove(at: i)
                currentRecentRecords.insert(word, at: 0)
            }
            
        }else{
            if currentRecentRecords.count == maxSize {
                currentRecentRecords.removeLast()
            }
            currentRecentRecords.insert(word, at: 0)
        }
        
        Utility.PreferenceManager.recentRecords = currentRecentRecords
    }
    
    /// 최근 검색어를 삭제
    /// - Parameter word: 최근 검색어
    func removeRecentRecords(word: String) {
        var currentRecentRecords = Utility.PreferenceManager.recentRecords ?? []

        if let i = currentRecentRecords.firstIndex(where: { $0 == word }){
            currentRecentRecords.remove(at: i)
        }
        
        Utility.PreferenceManager.recentRecords = currentRecentRecords
    }
}
