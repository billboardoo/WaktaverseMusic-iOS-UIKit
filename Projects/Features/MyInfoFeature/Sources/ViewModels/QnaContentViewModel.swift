import BaseFeature
import MyInfoFeatureInterface
import Foundation
import RxRelay
import RxSwift
import Utility

public final class QnaContentViewModel: ViewModelType {
    var disposeBag = DisposeBag()
    var dataSource: [FaqModel]

    public struct Input {}

    public struct Output {}

    public init(
        dataSource: [FaqModel]
    ) {
        DEBUG_LOG("✅ \(Self.self) 생성")
        self.dataSource = dataSource
    }

    public func transform(from input: Input) -> Output {
        let output = Output()
        return output
    }
}
