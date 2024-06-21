import BaseFeature
import Foundation
import KeychainModule
import MainTabFeature
import MyInfoFeature
import NeedleFoundation
import RootFeature
import StorageFeature
import UIKit

public final class AppComponent: BootstrapComponent {
    public func makeRootView() -> IntroViewController {
        rootComponent.makeView()
    }

    public var keychain: any Keychain {
        shared {
            KeychainImpl()
        }
    }

    var rootComponent: RootComponent {
        shared {
            RootComponent(parent: self)
        }
    }
}

// MARK: - Tabbar
public extension AppComponent {
    var mainContainerComponent: MainContainerComponent {
        MainContainerComponent(parent: self)
    }

    var bottomTabBarComponent: BottomTabBarComponent {
        BottomTabBarComponent(parent: self)
    }

    var mainTabBarComponent: MainTabBarComponent {
        MainTabBarComponent(parent: self)
    }
}

// MARK: - ETC
public extension AppComponent {
    var permissionComponent: PermissionComponent {
        PermissionComponent(parent: self)
    }
}
