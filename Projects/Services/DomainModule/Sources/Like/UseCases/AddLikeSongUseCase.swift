//
//  FetchArtistListUseCase.swift
//  DomainModule
//
//  Created by KTH on 2023/02/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import DataMappingModule
import Foundation
import RxSwift

public protocol AddLikeSongUseCase {
    func execute(id: String) -> Single<LikeEntity>
}
