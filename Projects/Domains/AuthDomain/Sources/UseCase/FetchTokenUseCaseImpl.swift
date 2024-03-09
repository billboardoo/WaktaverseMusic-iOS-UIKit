//
//  FetchArtistListUseCaseImpl.swift
//  DataModule
//
//  Created by KTH on 2023/02/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import AuthDomainInterface
import Foundation
import RxSwift

public struct FetchTokenUseCaseImpl: FetchTokenUseCase {
    private let authRepository: any AuthRepository

    public init(authRepository: any AuthRepository) {
        self.authRepository = authRepository
    }

    public func execute(token: String, type: ProviderType) -> Single<AuthLoginEntity> {
        authRepository.fetchToken(token: token, type: type)
    }
}
