import Foundation
import LinkPresentation
import UIKit

private enum OpenerPlatform {
    case youtube
    case youtubeMusic
}

private enum VideoPlayType {
    case videos(ids: [String])
    case playlist(listID: String)
}

public struct WakmusicYoutubePlayer: WakmusicPlayer {
    private let youtubeURLGenerator: any YoutubeURLGeneratable
    private let youtubeVideoType: VideoPlayType
    private var openerPlatform: OpenerPlatform {
        let platform = PreferenceManager.songPlayPlatformType ?? .youtube
        switch platform {
        case .youtube: return .youtube
        case .youtubeMusic: return .youtubeMusic
        }
    }

    public init(
        id: String,
        youtubeURLGenerator: any YoutubeURLGeneratable = YoutubeURLGenerator()
    ) {
        self.youtubeVideoType = .videos(ids: [id])
        self.youtubeURLGenerator = youtubeURLGenerator
    }

    public init(
        ids: [String],
        youtubeURLGenerator: any YoutubeURLGeneratable = YoutubeURLGenerator()
    ) {
        self.youtubeVideoType = .videos(ids: ids)
        self.youtubeURLGenerator = youtubeURLGenerator
    }

    public init(
        listID: String,
        youtubeURLGenerator: any YoutubeURLGeneratable = YoutubeURLGenerator()
    ) {
        self.youtubeVideoType = .playlist(listID: listID)
        self.youtubeURLGenerator = youtubeURLGenerator
    }

    public func play() {
        switch youtubeVideoType {
        case let .videos(ids):
            playYoutube(ids: ids)
        case let .playlist(listID):
            playPlaylistYoutube(listID: listID)
        }
    }
}

private extension WakmusicYoutubePlayer {
    func playYoutube(ids: [String]) {
        Task { @MainActor in
            guard let url = await urlForYoutube(ids: ids) else {
                return
            }
            await UIApplication.shared.open(url)
        }
    }

    func playPlaylistYoutube(listID: String) {
        guard let url = urlForYoutubePlaylist(listID: listID) else {
            return
        }
        UIApplication.shared.open(url)
    }
}

private extension WakmusicYoutubePlayer {
    func urlForYoutube(ids: [String]) async -> URL? {
        do {
            switch openerPlatform {
            case .youtube:
                return if let appURL = urlForYoutubeApp(ids: ids) {
                    appURL
                } else if let webURL = urlForYoutubeWeb(ids: ids) {
                    webURL
                } else {
                    nil
                }

            case .youtubeMusic:
                return if let appURL = try await urlForYoutubeMusicApp(ids: ids) {
                    appURL
                } else if let webURL = try await urlForYoutubeMusicWeb(ids: ids) {
                    webURL
                } else {
                    nil
                }
            }
        } catch {
            print(error)
            return nil
        }
    }

    func urlForYoutubeApp(ids: [String]) -> URL? {
        return openableURL(
            youtubeURLGenerator.generateYoutubeVideoAppURL(ids: ids)
        )
    }

    func urlForYoutubeWeb(ids: [String]) -> URL? {
        return openableURL(
            youtubeURLGenerator.generateYoutubeVideoWebURL(ids: ids)
        )
    }

    func urlForYoutubeMusicApp(ids: [String]) async throws -> URL? {
        if ids.count == 1, let id = ids.first {
            return openableURL(youtubeURLGenerator.generateYoutubeMusicVideoAppURL(id: id))
        } else if let redirectedYoutubeURL = try await redirectedYoutubeURL(
            youtubeURLGenerator
                .generateYoutubeVideoWebURL(ids: ids)
        ),
            let components = URLComponents(url: redirectedYoutubeURL, resolvingAgainstBaseURL: false),
            let listID = components.queryItems?.first(where: { $0.name == "list" })?.value {
            return openableURL(youtubeURLGenerator.generateYoutubeMusicPlaylistAppURL(id: listID))
        }
        return nil
    }

    func urlForYoutubeMusicWeb(ids: [String]) async throws -> URL? {
        if ids.count == 1, let id = ids.first {
            return openableURL(youtubeURLGenerator.generateYoutubeMusicVideoWebURL(id: id))
        } else if let redirectedYoutubeURL = try await redirectedYoutubeURL(
            youtubeURLGenerator
                .generateYoutubeVideoWebURL(ids: ids)
        ),
            let components = URLComponents(url: redirectedYoutubeURL, resolvingAgainstBaseURL: false),
            let listID = components.queryItems?.first(where: { $0.name == "list" })?.value {
            return openableURL(youtubeURLGenerator.generateYoutubeMusicPlaylistWebURL(id: listID))
        }
        return nil
    }
}

private extension WakmusicYoutubePlayer {
    func urlForYoutubePlaylist(listID: String) -> URL? {
        switch openerPlatform {
        case .youtube:
            let appURL = openableURL(youtubeURLGenerator.generateYoutubePlaylistAppURL(id: listID))
            let webURL = openableURL(youtubeURLGenerator.generateYoutubePlaylistWebURL(id: listID))
            return appURL ?? webURL

        case .youtubeMusic:
            let appURL = openableURL(youtubeURLGenerator.generateYoutubeMusicPlaylistAppURL(id: listID))
            let webURL = openableURL(youtubeURLGenerator.generateYoutubeMusicPlaylistWebURL(id: listID))
            return appURL ?? webURL
        }
    }
}

private extension WakmusicYoutubePlayer {
    func openableURL(_ urlString: String) -> URL? {
        guard let url = URL(string: urlString), UIApplication.shared.canOpenURL(url) else { return nil }
        return url
    }

    @MainActor
    func redirectedYoutubeURL(_ urlString: String) async throws -> URL? {
        guard let url = URL(string: urlString) else { return nil }

        let provider = LPMetadataProvider()
        let metadata = try await provider.startFetchingMetadata(for: url)
        guard let redirectedURL = metadata.url,
              UIApplication.shared.canOpenURL(redirectedURL)
        else { return nil }

        return redirectedURL
    }
}
