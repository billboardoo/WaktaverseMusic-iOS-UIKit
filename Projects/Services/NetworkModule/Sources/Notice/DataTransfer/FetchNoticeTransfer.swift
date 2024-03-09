//
//  FetchNoticeTransfer.swift
//  NetworkModuleTests
//
//  Created by KTH on 2023/04/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import DataMappingModule
import DomainModule
import Foundation
import Utility

public extension FetchNoticeResponseDTO {
    func toDomain() -> FetchNoticeEntity {
        FetchNoticeEntity(
            id: id,
            category: category ?? "",
            title: title,
            thumbnail: thumbnail,
            content: content,
            images: images,
            createdAt: createdAt,
            startAt: startAt,
            endAt: endAt
        )
    }
}
