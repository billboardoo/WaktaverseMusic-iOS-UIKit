//
//  PlayerUserCase+Protocol.swift
//  PlayerFeature
//
//  Created by YoungK on 2023/01/13.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import RxSwift

public protocol PlayerUseCase {
    func next()
    func prev()
}
