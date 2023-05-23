//
//  CheckVersionResponseDTO.swift
//  DataMappingModuleTests
//
//  Created by yongbeomkwak on 2023/05/23.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation

public struct CheckVersionResponseDTO: Codable {
    public let flag: Int
    public let title,description,version: String?
}
