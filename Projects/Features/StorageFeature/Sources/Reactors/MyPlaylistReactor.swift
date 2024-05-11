import Foundation
import ReactorKit
import RxSwift
import LogManager

final class MyPlaylistReactor: Reactor {
    
    enum Action {
        
    }
    
    enum Mutation {
        
    }
    
    struct State {
        
    }
    
    var initialState: State
    
    init() {
        self.initialState = State (
        )
    }
    
    deinit {
        LogManager.printDebug("❌ Deinit \(Self.self)")
    }
    
    func mutate(action: Action) -> Observable<Mutation> {
        
        switch action {
            
        }
        
    }
    
    func reduce(state: State, mutation: Mutation) -> State {
        
        var newState = state
        
        switch mutation {
            
        }
        
        
        return newState
    }
    
}
