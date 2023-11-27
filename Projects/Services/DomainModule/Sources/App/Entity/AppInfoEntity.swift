//
//  VersionCheckEntity.swift
//  DomainModuleTests
//
//  Created by yongbeomkwak on 2023/05/23.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import DataMappingModule

public struct AppInfoEntity: Equatable {
    public init(
        flag: AppInfoFlagType,
        title: String,
        description: String,
        version: String,
        specialLogo: Bool
    )
    {
        self.flag = flag
        self.title = title
        self.description = description
        self.version = version
        self.specialLogo = specialLogo
    }
    
    public let flag: AppInfoFlagType
    public let title, description, version: String
    public let specialLogo: Bool
}
