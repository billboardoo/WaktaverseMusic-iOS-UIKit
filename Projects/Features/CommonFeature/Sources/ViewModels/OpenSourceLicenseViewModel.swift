//
//  OpenSourceLicenseViewModel.swift
//  CommonFeature
//
//  Created by KTH on 2023/05/17.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation
import RxSwift
import RxCocoa
import Utility

public final class OpenSourceLicenseViewModel {
    let input = Input()
    let output = Output()
    var disposeBag = DisposeBag()
    
    public struct Input {}
    public struct Output {
        var dataSource: BehaviorRelay<[OpenSourceLicense]> = BehaviorRelay(value: [])
    }
    
    init() {
        var dataSource: [OpenSourceLicense] = [
            OpenSourceLicense(
                title: "Alamofire",
                description: "The MIT License\nCopyright (c) 2014-2018 Alamofire Software Foundation",
                link: "https://github.com/Alamofire/Alamofire"
            ),
            OpenSourceLicense(
                title: "Moya",
                description: "The MIT License\nCopyright (c) 2014-present Artsy, Ash Furrow",
                link: "https://github.com/Moya/Moya.git"
            ),
            OpenSourceLicense(
                title: "Kingfisher",
                description: "The MIT License\nCopyright (c) 2018 Wei Wang",
                link: "https://github.com/onevcat/Kingfisher.git"
            ),
            OpenSourceLicense(
                title: "RxSwift",
                description: "The MIT License\nCopyright © 2015 Krunoslav Zaher All rights reserved.",
                link: "https://github.com/ReactiveX/RxSwift.git"
            ),
            OpenSourceLicense(
                title: "RxDataSources",
                description: "The MIT License\nCopyright (c) 2017 RxSwift Community",
                link: "https://github.com/RxSwiftCommunity/RxDataSources.git"
            ),
            OpenSourceLicense(
                title: "RxKeyboard",
                description: "The MIT License\nCopyright (c) 2016 Suyeol Jeon (xoul.kr)",
                link: "https://github.com/RxSwiftCommunity/RxKeyboard.git"
            ),
            OpenSourceLicense(
                title: "PanModal",
                description: "The MIT License\nCopyright © 2018 Tiny Speck, Inc.",
                link: "https://github.com/slackhq/PanModal.git"
            ),
            OpenSourceLicense(
                title: "Then",
                description: "The MIT License\nCopyright (c) 2015 Suyeol Jeon (xoul.kr)",
                link: "https://github.com/devxoul/Then"
            ),
            OpenSourceLicense(
                title: "SnapKit",
                description: "The MIT License\nCopyright (c) 2011-Present SnapKit Team",
                link: "https://github.com/SnapKit/SnapKit.git"
            ),
            OpenSourceLicense(
                title: "Reachability",
                description: "The MIT License\nCopyright (c) 2016 Ashley Mills",
                link: "https://github.com/ashleymills/Reachability.swift"
            ),
            OpenSourceLicense(
                title: "Lottie-ios",
                description: "Apache License 2.0\nCopyright 2018 Airbnb, Inc.",
                link: "https://github.com/airbnb/lottie-ios.git"
            ),
            OpenSourceLicense(
                title: "Needle",
                description: "uber/needle is licensed under the Apache License 2.0",
                link: "https://github.com/uber/needle.git"
            ),
            OpenSourceLicense(
                title: "Tabman",
                description: "The MIT License\nCopyright (c) 2022 UI At Six",
                link: "https://github.com/uias/Tabman.git"
            ),
            OpenSourceLicense(
                title: "SwiftEntryKit",
                description: "The MIT License\nCopyright (c) 2018 Daniel Huri",
                link: "https://github.com/huri000/SwiftEntryKit"
            ),
            OpenSourceLicense(
                title: "Naveridlogin-sdk-ios",
                description: "naver/naveridlogin-sdk-ios is licensed under the Apache License 2.0",
                link: "https://github.com/naver/naveridlogin-sdk-ios"
            ),
            OpenSourceLicense(
                title: "CryptoSwift",
                description: "The MIT License\nCopyright (C) 2014-3099 Marcin Krzyżanowski",
                link: "https://github.com/krzyzanowskim/CryptoSwift.git"
            ),
            OpenSourceLicense(
                title: "MarqueeLabel",
                description: "The MIT License\nCopyright (c) 2011-2017 Charles Powell",
                link: "https://github.com/cbpowell/MarqueeLabel.git"
            ),
            OpenSourceLicense(
                title: "Firebase-ios-sdk",
                description: "firebase/firebase-ios-sdk is licensed under the Apache License 2.0",
                link: "https://github.com/firebase/firebase-ios-sdk.git"
            ),
            OpenSourceLicense(
                title: "NVActivityIndicatorView",
                description: "The MIT License\nCopyright (c) 2016 Vinh Nguyen",
                link: "https://github.com/ninjaprox/NVActivityIndicatorView.git"
            ),
            OpenSourceLicense(
                title: "RealmSwift",
                description: "realm/realm-swift is licensed under the Apache License 2.0",
                link: "https://github.com/realm/realm-swift"
            ),
            OpenSourceLicense(
                title: "Amplify",
                description: "aws-amplify/amplify-swift is licensed under the Apache License 2.0",
                link: "https://github.com/aws-amplify/amplify-swift.git"
            ),
            OpenSourceLicense(
                title: "YouTubePlayerKit",
                description: "The MIT License\nCopyright (c) 2023 Sven Tiigi",
                link: "https://github.com/SvenTiigi/YouTubePlayerKit.git"
            )
        ].sorted { $0.title < $1.title }

        self.loadTextFileFromBundle(fileName: "ApacheLicense") { [weak self] (result) in
            guard let self else { return }
            switch result {
            case .success(let contents):
                let apacheLicense = OpenSourceLicense(
                    type: .license,
                    title: "Apache License 2.0",
                    description: contents,
                    link: ""
                )
                dataSource.append(apacheLicense)

                self.loadTextFileFromBundle(fileName: "MITLicense") { [weak self] (result) in
                    guard let self else { return }
                    switch result {
                    case .success(let contents):
                        let mitLicense = OpenSourceLicense(
                            type: .license,
                            title: "MIT License (MIT)",
                            description: contents,
                            link: ""
                        )
                        dataSource.append(mitLicense)
                        self.output.dataSource.accept(dataSource)

                    case .failure(let error):
                        DEBUG_LOG("파일 로드 에러: \(error)")
                    }
                }
            case .failure(let error):
                DEBUG_LOG("파일 로드 에러: \(error)")
            }
        }
    }
}

extension OpenSourceLicenseViewModel {
    private func loadTextFileFromBundle(fileName: String, completionHandler: @escaping (Result<String, Error>) -> Void) {
        DispatchQueue.global().async {
            if let fileURL = CommonFeatureResources.bundle.url(forResource: fileName, withExtension: "txt") {
                do {
                    let contents = try String(contentsOf: fileURL, encoding: .utf8)
                    completionHandler(.success(contents))
                } catch {
                    completionHandler(.failure(error))
                }
            } else {
                let error = NSError(domain: "com.example.app", code: 0, userInfo: [NSLocalizedDescriptionKey: "파일을 찾을 수 없습니다."])
                completionHandler(.failure(error))
            }
        }
    }
}

public enum OpenSourceLicenseType {
    case library
    case license
}

public struct OpenSourceLicense {
    public var type: OpenSourceLicenseType = .library
    public let title: String
    public let description: String
    public let link: String
}
