//
//  WMImageAPI.swift
//  Utility
//
//  Created by KTH on 2023/02/10.
//  Copyright Â© 2023 yongbeomkwak. All rights reserved.
//

import Foundation

public enum WMImageAPI {
    case fetchNewsThumbnail(time: String)
    case fetchArtistWithRound(id: String, version: Int)
    case fetchArtistWithSquare(id: String, version: Int)
    case fetchProfile(name: String, version: Int)
    case fetchPlayList(id: String, version: Int)
    case fetchRecommendPlayListWithRound(id: String, version: Int)
    case fetchRecommendPlayListWithSquare(id: String, version: Int)
    case fetchYoutubeThumbnail(id: String)
    case fetchNotice(id: String)
}

extension WMImageAPI {
    public var baseURLString: String {
        return BASE_IMAGE_URL()
    }

    public var youtubeBaseURLString: String {
        return "https://i.ytimg.com"
    }
    
    public var path: String {
        switch self {
        case let .fetchNewsThumbnail(time):
            return WMDOMAIN_IMAGE_NEWS() + "/\(time).png"
            
        case let .fetchArtistWithRound(id, version):
            return WMDOMAIN_IMAGE_ARTIST_ROUND() + "/\(id).png?v=\(version)"
            
        case let .fetchArtistWithSquare(id, version):
            return WMDOMAIN_IMAGE_ARTIST_SQUARE() + "/\(id).png?v=\(version)"
            
        case let .fetchProfile(name,version):
            return WMDOMAIN_IMAGE_PROFILE() + "/\(name).png?v=\(version)"
            
        case let .fetchPlayList(id,version):
            return WMDOMAIN_IMAGE_PLAYLIST() + "/\(id).png?v=\(version)"
            
        case let .fetchRecommendPlayListWithSquare(id,version):
            return WMDOMAIN_IMAGE_RECOMMEND_PLAYLIST_SQUARE() + "/\(id).png?v=\(version)"
            
        case let .fetchRecommendPlayListWithRound(id,version):
            return WMDOMAIN_IMAGE_RECOMMEND_PLAYLIST_ROUND() + "/\(id).png?v=\(version)"
            
        case let .fetchYoutubeThumbnail(id):
            return "vi/\(id)/hqdefault.jpg"
            
        case let .fetchNotice(id):
            return WMDOMAIN_IMAGE_NOTICE() + "/\(id)"
        }
    }
    
    public var toString: String {
        switch self {
        case .fetchYoutubeThumbnail:
            return youtubeBaseURLString + "/" + path
        default:
            return baseURLString + "/" + path
        }
    }
    
    public var toURL: URL? {
        switch self {
        case .fetchYoutubeThumbnail:
            return URL(string: youtubeBaseURLString + "/" + path)
        default:
            return URL(string: baseURLString + "/" + path)
        }
    }
}
