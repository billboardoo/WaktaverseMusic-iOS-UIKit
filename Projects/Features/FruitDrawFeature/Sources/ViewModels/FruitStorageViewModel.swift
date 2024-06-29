import AuthDomainInterface
import BaseFeature
import Foundation
import LogManager
import RxRelay
import RxSwift
import UserDomainInterface
import Utility

public final class FruitStorageViewModel: ViewModelType {
    private let fetchFruitListUseCase: FetchFruitListUseCase
    private let logoutUseCase: LogoutUseCase
    private let disposeBag = DisposeBag()

    deinit {
        LogManager.printDebug("❌:: \(Self.self) deinit")
    }

    public init(
        fetchFruitListUseCase: any FetchFruitListUseCase,
        logoutUseCase: any LogoutUseCase
    ) {
        self.fetchFruitListUseCase = fetchFruitListUseCase
        self.logoutUseCase = logoutUseCase
    }

    public struct Input {
        let fetchFruitList: PublishSubject<Void> = PublishSubject()
    }

    public struct Output {
        let fruitSource: BehaviorRelay<[[FruitEntity]]> = BehaviorRelay(value: [])
    }

    public func transform(from input: Input) -> Output {
        let output = Output()

        input.fetchFruitList
            .flatMap { [fetchFruitListUseCase] _ in
                return fetchFruitListUseCase.execute()
                    .asObservable()
                    .catchAndReturn([])
            }
            .map { [weak self] source in
                guard let self = self else { return [] }
                return self.chunkArray(source: source, chunkSize: 3)
            }
            .bind(to: output.fruitSource)
            .disposed(by: disposeBag)

        return output
    }
}

private extension FruitStorageViewModel {
    func chunkArray(source: [FruitEntity], chunkSize: Int) -> [[FruitEntity]] {
        guard chunkSize > 0 else { return [] }

        var result: [[FruitEntity]] = []
        var chunk: [FruitEntity] = []

        for element in source {
            chunk.append(element)
            if chunk.count == chunkSize {
                result.append(chunk)
                chunk = []
            }
        }

        if !chunk.isEmpty {
            result.append(chunk)
        }
        return result
    }
}
