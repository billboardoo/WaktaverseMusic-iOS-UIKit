//
//  PlayButtonDelegate.swift
//  Utility
//
//  Created by yongbeomkwak on 2023/03/23.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import SongsDomainInterface
import Foundation

public protocol PlayButtonDelegate: AnyObject {
    func play(model: SongEntity)
}
