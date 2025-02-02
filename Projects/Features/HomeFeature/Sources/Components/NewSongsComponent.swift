import Foundation
import NeedleFoundation

public protocol NewSongsDependency: Dependency {
    var newSongsContentComponent: NewSongsContentComponent { get }
}

@MainActor
public final class NewSongsComponent: Component<NewSongsDependency> {
    public func makeView() -> NewSongsViewController {
        return NewSongsViewController.viewController(newSongsContentComponent: dependency.newSongsContentComponent)
    }
}
