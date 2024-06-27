import AuthDomainInterface
import BaseDomainInterface
import BaseFeatureInterface
import Foundation
import PlaylistDomainInterface
import RxRelay
import RxSwift
import UserDomainInterface
import Utility

public final class MultiPurposePopupViewModel: ViewModelType {
    var type: PurposeType
    var key: String



    public struct Input {
        let textString: BehaviorRelay<String> = BehaviorRelay(value: "")
    }

    public struct Output {
    }

    public init(
        type: PurposeType,
        key: String
    ) {
        self.key = key
        self.type = type
    }

    deinit {
        DEBUG_LOG("❌ \(Self.self) 소멸")
    }

    public func transform(from input: Input) -> Output {
        var output = Output()


        return output
    }
}
