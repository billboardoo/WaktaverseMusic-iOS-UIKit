//
//  UserInfoEntity.swift
//  DomainModule
//
//  Created by yongbeomkwak on 12/8/23.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation

public struct UserInfoEntity: Equatable {
    
    public init(
        id:String,
        platform:String,
        name:String,
        profile:String,
        version:Int

    ) {
        self.id = id
        self.platform = platform
        self.name = name
        self.profile = profile
        self.version = version
    }
    
    public let id, platform, name,profile:String
    public let version:Int
    
    
    public static func == (lhs: UserInfoEntity, rhs: UserInfoEntity) -> Bool {
        lhs.id == rhs.id
    }
   
}
