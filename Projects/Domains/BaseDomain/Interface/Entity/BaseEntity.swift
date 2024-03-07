//
//  BaseEntity.swift
//  BaseDomain
//
//  Created by KTH on 2024/03/04.
//  Copyright © 2024 yongbeomkwak. All rights reserved.
//

import Foundation

public struct BaseEntity {
    public init(
        status: Int,
        description: String = ""
    ) {
        self.status = status
        self.description = description
    }

    public let status: Int
    public var description: String = ""
}
