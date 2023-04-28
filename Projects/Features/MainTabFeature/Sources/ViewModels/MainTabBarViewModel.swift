//
//  MainTabBarViewModel.swift
//  MainTabFeature
//
//  Created by KTH on 2023/04/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import RxSwift
import RxCocoa
import DomainModule
import DataMappingModule
import Utility

public class MainTabBarViewModel {
    
    var input = Input()
    var output = Output()
    var disposeBag = DisposeBag()
    
    var fetchNoticeUseCase: FetchNoticeUseCase
    
    public struct Input {
    }

    public struct Output {
        var dataSource: BehaviorRelay<[FetchNoticeEntity]> = BehaviorRelay(value: [])
    }
    
    public init(
        fetchNoticeUseCase: any FetchNoticeUseCase
    ){
        self.fetchNoticeUseCase = fetchNoticeUseCase
        
        let igoredNoticeIds: [Int] = Utility.PreferenceManager.ignoredNoticeIDs ?? []
        DEBUG_LOG("igoredNoticeIds: \(igoredNoticeIds)")
        
        self.fetchNoticeUseCase.execute(type: .currently)
            .catchAndReturn([])
            .asObservable()
            .map { $0.sorted { $0.id > $1.id } }
            .map{ (entities) in
                guard !igoredNoticeIds.isEmpty else { return entities }
                return entities.filter { entity in
                    return !igoredNoticeIds.contains(where: { $0 == entity.id })
                }
            }
            .debug("igoredNoticeIds")
            .bind(to: output.dataSource)
            .disposed(by: disposeBag)
    }
}
