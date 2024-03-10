//
//  FetchProfileListUseCaseImpl.swift
//  DataModule
//
//  Created by KTH on 2023/02/18.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import UserDomainInterface
import Foundation
import RxSwift

public struct FetchProfileListUseCaseImpl: FetchProfileListUseCase {
    private let userRepository: any UserRepository

    public init(
        userRepository: UserRepository
    ) {
        self.userRepository = userRepository
    }

    public func execute() -> Single<[ProfileListEntity]> {
        userRepository.fetchProfileList()
    }
}
