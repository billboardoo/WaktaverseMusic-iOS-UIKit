//
//  NoticeRepository.swift
//  DomainModuleTests
//
//  Created by KTH on 2023/04/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import RxSwift
import DataMappingModule
import ErrorModule

public protocol NoticeRepository {
    func fetchNotice(type: NoticeType) -> Single<[FetchNoticeEntity]>
    func fetchNoticeCategories() -> Single<[FetchNoticeCategoriesEntity]>
}
