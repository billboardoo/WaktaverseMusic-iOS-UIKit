//
//  NaverUserInfoResponseDTO.swift
//  DataMappingModule
//
//  Created by yongbeomkwak on 2023/02/15.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation

public struct AuthUserInfoEntity: Equatable {
    
   
    
 
    public init(
        id:String,
        platform:String,
        displayName:String,
        first_login_time:Int,
        first:Bool,
        profile:String,
        version:Int

    ) {
        self.id = id
        self.platform = platform
        self.displayName = displayName
        self.first_login_time = first_login_time
        self.first = first
        self.profile = profile
        self.version = version
    }
    
    public let id, platform, displayName,profile:String
    public let first_login_time,version:Int
    public let first:Bool
    
    
    public static func == (lhs: AuthUserInfoEntity, rhs: AuthUserInfoEntity) -> Bool {
        lhs.id == rhs.id
    }
   
}



