import ProjectDescription

public extension TargetDependency {
    struct SPM {}
}

public extension TargetDependency.SPM {
    // MARK: External
    static let Moya = TargetDependency.external(name: "Moya")
    static let RxMoya = TargetDependency.external(name: "RxMoya")
    static let PanModal = TargetDependency.external(name: "PanModal")
    static let RxSwift = TargetDependency.external(name: "RxSwift")
    static let RxCocoa = TargetDependency.external(name: "RxCocoa")
    static let Kingfisher = TargetDependency.external(name: "Kingfisher")
    static let Then = TargetDependency.external(name: "Then")
    static let SnapKit = TargetDependency.external(name: "SnapKit")
    static let ReachabilitySwift = TargetDependency.external(name: "ReachabilitySwift")
    static let Lottie = TargetDependency.external(name: "Lottie")
    static let Needle = TargetDependency.external(name: "NeedleFoundation")
    static let Tabman = TargetDependency.external(name: "Tabman")
    static let RxDataSources = TargetDependency.external(name: "RxDataSources")
    static let RxKeyboard = TargetDependency.external(name: "RxKeyboard")
    static let SwiftEntryKit = TargetDependency.external(name: "SwiftEntryKit")
    static let NaverLogin = TargetDependency.external(name: "naveridlogin-ios-sp")
    static let CryptoSwift = TargetDependency.external(name: "CryptoSwift")
    static let MarqueeLabel = TargetDependency.external(name: "MarqueeLabel")
    static let FirebaseAnalytics = TargetDependency.external(name: "FirebaseAnalyticsWithoutAdIdSupport")
    static let FirebaseCrashlytics = TargetDependency.external(name: "FirebaseCrashlytics")
    static let NVActivityIndicatorView = TargetDependency.external(name: "NVActivityIndicatorView")
    static let ReactorKit = TargetDependency.external(name: "ReactorKit")
    static let Quick = TargetDependency.external(name: "Quick")
    static let Nimble = TargetDependency.external(name: "Nimble")
    static let Inject = TargetDependency.external(name: "Inject")
    static let Realm = TargetDependency.external(name: "Realm")
    static let RealmSwift = TargetDependency.external(name: "RealmSwift")

// MARK: Native SPM
    static let YouTubePlayerKit = TargetDependency.package(product: "YouTubePlayerKit")
  //  static let Amplify = TargetDependency.package(product: "Amplify")
 //   static let AWSCognitoAuthPlugin = TargetDependency.package(product: "AWSCognitoAuthPlugin")
 //   static let AWSS3StoragePlugin = TargetDependency.package(product:"AWSS3StoragePlugin")
}

public extension Package {
    static let YouTubePlayerKit = Package.remote(
        url: "https://github.com/SvenTiigi/YouTubePlayerKit.git",
        requirement: .upToNextMajor(from: "1.3.1")
    )
    
    /*
    static let Amplify = Package.remote(
        url: "https://github.com/aws-amplify/amplify-swift.git",
        requirement: .upToNextMajor(from: "2.10.0")
    )
     */
}

