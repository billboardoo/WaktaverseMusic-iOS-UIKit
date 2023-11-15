//
//  SecretURL.swift
//  APIKit
//
//  Created by KTH on 2023/04/25.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import Foundation

public func config(key: String) -> String {
    guard let secrets = Bundle.main.object(forInfoDictionaryKey: "Secrets") as? [String: Any] else {
        return ""
    }
    return secrets[key] as? String ?? "not found key"
}

//MARK: - BASE_URL
public func BASE_URL() -> String {
    #if DEBUG
    return config(key: "BASE_DEV_URL")
    #else
    return config(key: "BASE_PROD_URL")
    #endif
}

//MARK: - WAKENTER_BASE_URL
public func WAKENTER_BASE_URL() -> String {
    return config(key: "WAKENTER_BASE_URL")
}

//MARK: - WMDomain
public func WMDOMAIN_AUTH() -> String {
    return config(key: "WMDOMAIN_AUTH")
}
public func WMDOMAIN_CHARTS() -> String {
    return config(key: "WMDOMAIN_CHARTS")
}
public func WMDOMAIN_SONGS() -> String {
    return config(key: "WMDOMAIN_SONGS")
}
public func WMDOMAIN_ARTIST() -> String {
    return config(key: "WMDOMAIN_ARTIST")
}
public func WMDOMAIN_USER() -> String {
    return config(key: "WMDOMAIN_USER")
}
public func WMDOMAIN_PLAYLIST() -> String {
    return config(key: "WMDOMAIN_PLAYLIST")
}
public func WMDOMAIN_LIKE() -> String {
    return config(key: "WMDOMAIN_LIKE")
}
public func WMDOMAIN_QNA() -> String {
    return config(key: "WMDOMAIN_QNA")
}
public func WMDOMAIN_NOTICE() -> String {
    return config(key: "WMDOMAIN_NOTICE")
}
public func WMDOMAIN_SUGGEST() -> String {
    return config(key: "WMDOMAIN_SUGGEST")
}
public func WMDOMAIN_APP() -> String {
    return config(key: "WMDOMAIN_APP")
}
public func WMDOMAIN_PLAY() -> String {
    return config(key: "WMDOMAIN_PLAY")
}

//MARK: - WMDomain V2
public func WMDOMAIN_V2_SONGS() -> String {
    return config(key: "WMDOMAIN_V2_SONGS")
}
