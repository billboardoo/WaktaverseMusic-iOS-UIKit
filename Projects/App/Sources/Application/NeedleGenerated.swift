

import ArtistFeature
import BaseFeature
import ChartFeature
import CommonFeature
import DataMappingModule
import DataModule
import DesignSystem
import DomainModule
import Foundation
import KeychainModule
import MainTabFeature
import NeedleFoundation
import NetworkModule
import PanModal
import PlayerFeature
import RootFeature
import RxCocoa
import RxKeyboard
import RxSwift
import SearchFeature
import SignInFeature
import SnapKit
import StorageFeature
import UIKit
import Utility

// swiftlint:disable unused_declaration
private let needleDependenciesHash : String? = nil

// MARK: - Traversal Helpers

private func parent1(_ component: NeedleFoundation.Scope) -> NeedleFoundation.Scope {
    return component.parent
}

// MARK: - Providers

#if !NEEDLE_DYNAMIC

private class ArtistDependency132a213bf62ad60c622cProvider: ArtistDependency {
    var fetchArtistListUseCase: any FetchArtistListUseCase {
        return appComponent.fetchArtistListUseCase
    }
    var artistDetailComponent: ArtistDetailComponent {
        return appComponent.artistDetailComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ArtistComponent
private func factorye0c5444f5894148bdd93f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ArtistDependency132a213bf62ad60c622cProvider(appComponent: parent1(component) as! AppComponent)
}
private class ArtistDetailDependencyee413dcf7a70e89df6d9Provider: ArtistDetailDependency {
    var artistMusicComponent: ArtistMusicComponent {
        return appComponent.artistMusicComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ArtistDetailComponent
private func factory35314797fadaf164ece6f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ArtistDetailDependencyee413dcf7a70e89df6d9Provider(appComponent: parent1(component) as! AppComponent)
}
private class ArtistMusicContentDependency1615ac8469e54ec51921Provider: ArtistMusicContentDependency {
    var fetchArtistSongListUseCase: any FetchArtistSongListUseCase {
        return appComponent.fetchArtistSongListUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ArtistMusicContentComponent
private func factory8b6ffa46033e2529b5daf47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ArtistMusicContentDependency1615ac8469e54ec51921Provider(appComponent: parent1(component) as! AppComponent)
}
private class ArtistMusicDependencya0f5073287829dfbc260Provider: ArtistMusicDependency {
    var artistMusicContentComponent: ArtistMusicContentComponent {
        return appComponent.artistMusicContentComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ArtistMusicComponent
private func factory382e7f8466df35a3f1d9f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ArtistMusicDependencya0f5073287829dfbc260Provider(appComponent: parent1(component) as! AppComponent)
}
private class PlaylistDependency6f376d117dc0f38671edProvider: PlaylistDependency {


    init() {

    }
}
/// ^->AppComponent->PlaylistComponent
private func factory3a0a6eb1061d8d5a2defe3b0c44298fc1c149afb(_ component: NeedleFoundation.Scope) -> AnyObject {
    return PlaylistDependency6f376d117dc0f38671edProvider()
}
private class PlayerDependencyf8a3d594cc3b9254f8adProvider: PlayerDependency {
    var fetchLyricsUseCase: any FetchLyricsUseCase {
        return appComponent.fetchLyricsUseCase
    }
    var playlistComponent: PlaylistComponent {
        return appComponent.playlistComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->PlayerComponent
private func factorybc7f802f601dd5913533f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return PlayerDependencyf8a3d594cc3b9254f8adProvider(appComponent: parent1(component) as! AppComponent)
}
private class MainTabBarDependencycd05b79389a6a7a6c20fProvider: MainTabBarDependency {
    var chartComponent: ChartComponent {
        return appComponent.chartComponent
    }
    var searchComponent: SearchComponent {
        return appComponent.searchComponent
    }
    var artistComponent: ArtistComponent {
        return appComponent.artistComponent
    }
    var storageComponent: StorageComponent {
        return appComponent.storageComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->MainTabBarComponent
private func factorye547a52b3fce5887c8c7f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return MainTabBarDependencycd05b79389a6a7a6c20fProvider(appComponent: parent1(component) as! AppComponent)
}
private class BottomTabBarDependency237c2bd1c7be62020295Provider: BottomTabBarDependency {


    init() {

    }
}
/// ^->AppComponent->BottomTabBarComponent
private func factoryd34fa9e493604a6295bde3b0c44298fc1c149afb(_ component: NeedleFoundation.Scope) -> AnyObject {
    return BottomTabBarDependency237c2bd1c7be62020295Provider()
}
private class MainContainerDependencyd9d908a1d0cf8937bbadProvider: MainContainerDependency {
    var bottomTabBarComponent: BottomTabBarComponent {
        return appComponent.bottomTabBarComponent
    }
    var mainTabBarComponent: MainTabBarComponent {
        return appComponent.mainTabBarComponent
    }
    var playerComponent: PlayerComponent {
        return appComponent.playerComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->MainContainerComponent
private func factory8e19f48d5d573d3ea539f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return MainContainerDependencyd9d908a1d0cf8937bbadProvider(appComponent: parent1(component) as! AppComponent)
}
private class ChartDependencyafd8882010751c9ef054Provider: ChartDependency {
    var chartContentComponent: ChartContentComponent {
        return appComponent.chartContentComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ChartComponent
private func factoryeac6a4df54bbd391d31bf47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ChartDependencyafd8882010751c9ef054Provider(appComponent: parent1(component) as! AppComponent)
}
private class ChartContentDependency3b8e41cfba060e4d16caProvider: ChartContentDependency {
    var fetchChartRankingUseCase: any FetchChartRankingUseCase {
        return appComponent.fetchChartRankingUseCase
    }
    var fetchChartUpdateTimeUseCase: any FetchChartUpdateTimeUseCase {
        return appComponent.fetchChartUpdateTimeUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ChartContentComponent
private func factoryc9a137630ce76907f36ff47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ChartContentDependency3b8e41cfba060e4d16caProvider(appComponent: parent1(component) as! AppComponent)
}
private class StorageDependency1447167c38e97ef97427Provider: StorageDependency {
    var signInComponent: SignInComponent {
        return appComponent.signInComponent
    }
    var afterLoginComponent: AfterLoginComponent {
        return appComponent.afterLoginComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->StorageComponent
private func factory2415399d25299b97b98bf47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return StorageDependency1447167c38e97ef97427Provider(appComponent: parent1(component) as! AppComponent)
}
private class MyPlayListDependency067bbf42b28f80e413acProvider: MyPlayListDependency {
    var multiPurposePopComponent: MultiPurposePopComponent {
        return appComponent.multiPurposePopComponent
    }
    var playListDetailComponent: PlayListDetailComponent {
        return appComponent.playListDetailComponent
    }
    var fetchPlayListUseCase: any FetchPlayListUseCase {
        return appComponent.fetchPlayListUseCase
    }
    var editPlayListOrderUseCase: any EditPlayListOrderUseCase {
        return appComponent.editPlayListOrderUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->MyPlayListComponent
private func factory51a57a92f76af93a9ec2f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return MyPlayListDependency067bbf42b28f80e413acProvider(appComponent: parent1(component) as! AppComponent)
}
private class AfterLoginDependencya880b76858e0a77ed700Provider: AfterLoginDependency {
    var fetchUserInfoUseCase: any FetchUserInfoUseCase {
        return appComponent.fetchUserInfoUseCase
    }
    var requestComponent: RequestComponent {
        return appComponent.requestComponent
    }
    var profilePopComponent: ProfilePopComponent {
        return appComponent.profilePopComponent
    }
    var myPlayListComponent: MyPlayListComponent {
        return appComponent.myPlayListComponent
    }
    var multiPurposePopComponent: MultiPurposePopComponent {
        return appComponent.multiPurposePopComponent
    }
    var favoriteComponent: FavoriteComponent {
        return appComponent.favoriteComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->AfterLoginComponent
private func factory6cc9c8141e04494113b8f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return AfterLoginDependencya880b76858e0a77ed700Provider(appComponent: parent1(component) as! AppComponent)
}
private class FavoriteDependency8f7fd37aeb6f0e5d0e30Provider: FavoriteDependency {
    var fetchFavoriteSongsUseCase: any FetchFavoriteSongsUseCase {
        return appComponent.fetchFavoriteSongsUseCase
    }
    var editFavoriteSongsOrderUseCase: any EditFavoriteSongsOrderUseCase {
        return appComponent.editFavoriteSongsOrderUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->FavoriteComponent
private func factory8e4acb90bd0d9b48604af47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return FavoriteDependency8f7fd37aeb6f0e5d0e30Provider(appComponent: parent1(component) as! AppComponent)
}
private class QnaDependencybc3f0a2d4f873ad1b160Provider: QnaDependency {
    var qnaContentComponent: QnaContentComponent {
        return appComponent.qnaContentComponent
    }
    var fetchQnaCategoriesUseCase: any FetchQnaCategoriesUseCase {
        return appComponent.fetchQnaCategoriesUseCase
    }
    var fetchQnaUseCase: any FetchQnaUseCase {
        return appComponent.fetchQnaUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->QnaComponent
private func factory49a98666675cb7a82038f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return QnaDependencybc3f0a2d4f873ad1b160Provider(appComponent: parent1(component) as! AppComponent)
}
private class RequestDependencyd4f6f0030dbf2a90cf21Provider: RequestDependency {
    var withdrawUserInfoUseCase: any WithdrawUserInfoUseCase {
        return appComponent.withdrawUserInfoUseCase
    }
    var qnaComponent: QnaComponent {
        return appComponent.qnaComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->RequestComponent
private func factory13954fb3ec537bab80bcf47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return RequestDependencyd4f6f0030dbf2a90cf21Provider(appComponent: parent1(component) as! AppComponent)
}
private class QnaContentDependency68ed55648233d525d265Provider: QnaContentDependency {


    init() {

    }
}
/// ^->AppComponent->QnaContentComponent
private func factory1501f7005831c8411229e3b0c44298fc1c149afb(_ component: NeedleFoundation.Scope) -> AnyObject {
    return QnaContentDependency68ed55648233d525d265Provider()
}
private class RootDependency3944cc797a4a88956fb5Provider: RootDependency {
    var mainContainerComponent: MainContainerComponent {
        return appComponent.mainContainerComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->RootComponent
private func factory264bfc4d4cb6b0629b40f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return RootDependency3944cc797a4a88956fb5Provider(appComponent: parent1(component) as! AppComponent)
}
private class SignInDependency5dda0dd015447272446cProvider: SignInDependency {
    var fetchTokenUseCase: any FetchTokenUseCase {
        return appComponent.fetchTokenUseCase
    }
    var fetchNaverUserInfoUseCase: any FetchNaverUserInfoUseCase {
        return appComponent.fetchNaverUserInfoUseCase
    }
    var fetchUserInfoUseCase: any FetchUserInfoUseCase {
        return appComponent.fetchUserInfoUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->SignInComponent
private func factoryda2925fd76da866a652af47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return SignInDependency5dda0dd015447272446cProvider(appComponent: parent1(component) as! AppComponent)
}
private class AfterSearchDependency61822c19bc2eb46d7c52Provider: AfterSearchDependency {
    var afterSearchContentComponent: AfterSearchContentComponent {
        return appComponent.afterSearchContentComponent
    }
    var fetchSearchSongUseCase: any FetchSearchSongUseCase {
        return appComponent.fetchSearchSongUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->AfterSearchComponent
private func factoryeb2da679e35e2c4fb9a5f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return AfterSearchDependency61822c19bc2eb46d7c52Provider(appComponent: parent1(component) as! AppComponent)
}
private class AfterSearchComponentDependency028b0697c8624344f660Provider: AfterSearchComponentDependency {


    init() {

    }
}
/// ^->AppComponent->AfterSearchContentComponent
private func factorycaaccdf52467bfa87f73e3b0c44298fc1c149afb(_ component: NeedleFoundation.Scope) -> AnyObject {
    return AfterSearchComponentDependency028b0697c8624344f660Provider()
}
private class SearchDependencya86903a2c751a4f762e8Provider: SearchDependency {
    var beforeSearchComponent: BeforeSearchComponent {
        return appComponent.beforeSearchComponent
    }
    var afterSearchComponent: AfterSearchComponent {
        return appComponent.afterSearchComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->SearchComponent
private func factorye3d049458b2ccbbcb3b6f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return SearchDependencya86903a2c751a4f762e8Provider(appComponent: parent1(component) as! AppComponent)
}
private class BeforeSearchDependencyebdecb1d478a4766488dProvider: BeforeSearchDependency {
    var playListDetailComponent: PlayListDetailComponent {
        return appComponent.playListDetailComponent
    }
    var fetchRecommendPlayListUseCase: any FetchRecommendPlayListUseCase {
        return appComponent.fetchRecommendPlayListUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->BeforeSearchComponent
private func factory9bb852337d5550979293f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return BeforeSearchDependencyebdecb1d478a4766488dProvider(appComponent: parent1(component) as! AppComponent)
}
private class MultiPurposePopDependency30141c7a9a9e67e148afProvider: MultiPurposePopDependency {
    var createPlayListUseCase: any CreatePlayListUseCase {
        return appComponent.createPlayListUseCase
    }
    var loadPlayListUseCase: any LoadPlayListUseCase {
        return appComponent.loadPlayListUseCase
    }
    var setUserNameUseCase: any SetUserNameUseCase {
        return appComponent.setUserNameUseCase
    }
    var editPlayListNameUseCase: any EditPlayListNameUseCase {
        return appComponent.editPlayListNameUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->MultiPurposePopComponent
private func factory972fcba2860fcb8ad7b8f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return MultiPurposePopDependency30141c7a9a9e67e148afProvider(appComponent: parent1(component) as! AppComponent)
}
private class PlayListDetailDependencyb06fb5392859952b82a2Provider: PlayListDetailDependency {
    var fetchPlayListDetailUseCase: any FetchPlayListDetailUseCase {
        return appComponent.fetchPlayListDetailUseCase
    }
    var editPlayListUseCase: any EditPlayListUseCase {
        return appComponent.editPlayListUseCase
    }
    var multiPurposePopComponent: MultiPurposePopComponent {
        return appComponent.multiPurposePopComponent
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->PlayListDetailComponent
private func factory9e077ee814ce180ea399f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return PlayListDetailDependencyb06fb5392859952b82a2Provider(appComponent: parent1(component) as! AppComponent)
}
private class ProfilePopDependency081172e20caa75abdb54Provider: ProfilePopDependency {
    var fetchProfileListUseCase: any FetchProfileListUseCase {
        return appComponent.fetchProfileListUseCase
    }
    var setProfileUseCase: any SetProfileUseCase {
        return appComponent.setProfileUseCase
    }
    private let appComponent: AppComponent
    init(appComponent: AppComponent) {
        self.appComponent = appComponent
    }
}
/// ^->AppComponent->ProfilePopComponent
private func factorybd14b11ccce6dac94a24f47b58f8f304c97af4d5(_ component: NeedleFoundation.Scope) -> AnyObject {
    return ProfilePopDependency081172e20caa75abdb54Provider(appComponent: parent1(component) as! AppComponent)
}

#else
extension AppComponent: Registration {
    public func registerItems() {

        localTable["keychain-any Keychain"] = { self.keychain as Any }
        localTable["searchComponent-SearchComponent"] = { self.searchComponent as Any }
        localTable["afterSearchComponent-AfterSearchComponent"] = { self.afterSearchComponent as Any }
        localTable["afterSearchContentComponent-AfterSearchContentComponent"] = { self.afterSearchContentComponent as Any }
        localTable["remoteSearchDataSource-any RemoteSearchDataSource"] = { self.remoteSearchDataSource as Any }
        localTable["songsRepository-any SongsRepository"] = { self.songsRepository as Any }
        localTable["fetchSearchSongUseCase-any FetchSearchSongUseCase"] = { self.fetchSearchSongUseCase as Any }
        localTable["fetchLyricsUseCase-any FetchLyricsUseCase"] = { self.fetchLyricsUseCase as Any }
        localTable["signInComponent-SignInComponent"] = { self.signInComponent as Any }
        localTable["storageComponent-StorageComponent"] = { self.storageComponent as Any }
        localTable["afterLoginComponent-AfterLoginComponent"] = { self.afterLoginComponent as Any }
        localTable["requestComponent-RequestComponent"] = { self.requestComponent as Any }
        localTable["remoteAuthDataSource-any RemoteAuthDataSource"] = { self.remoteAuthDataSource as Any }
        localTable["authRepository-any AuthRepository"] = { self.authRepository as Any }
        localTable["fetchTokenUseCase-any FetchTokenUseCase"] = { self.fetchTokenUseCase as Any }
        localTable["fetchNaverUserInfoUseCase-any FetchNaverUserInfoUseCase"] = { self.fetchNaverUserInfoUseCase as Any }
        localTable["fetchUserInfoUseCase-any FetchUserInfoUseCase"] = { self.fetchUserInfoUseCase as Any }
        localTable["withdrawUserInfoUseCase-any WithdrawUserInfoUseCase"] = { self.withdrawUserInfoUseCase as Any }
        localTable["remoteLikeDataSource-any RemoteLikeDataSource"] = { self.remoteLikeDataSource as Any }
        localTable["likeRepository-any LikeRepository"] = { self.likeRepository as Any }
        localTable["fetchLikeNumOfSongUseCase-any FetchLikeNumOfSongUseCase"] = { self.fetchLikeNumOfSongUseCase as Any }
        localTable["addLikeSongUseCase-any AddLikeSongUseCase"] = { self.addLikeSongUseCase as Any }
        localTable["cancelLikeSongUseCase-any CancelLikeSongUseCase"] = { self.cancelLikeSongUseCase as Any }
        localTable["beforeSearchComponent-BeforeSearchComponent"] = { self.beforeSearchComponent as Any }
        localTable["playListDetailComponent-PlayListDetailComponent"] = { self.playListDetailComponent as Any }
        localTable["multiPurposePopComponent-MultiPurposePopComponent"] = { self.multiPurposePopComponent as Any }
        localTable["myPlayListComponent-MyPlayListComponent"] = { self.myPlayListComponent as Any }
        localTable["remotePlayListDataSource-any RemotePlayListDataSource"] = { self.remotePlayListDataSource as Any }
        localTable["playListRepository-any PlayListRepository"] = { self.playListRepository as Any }
        localTable["fetchRecommendPlayListUseCase-any FetchRecommendPlayListUseCase"] = { self.fetchRecommendPlayListUseCase as Any }
        localTable["fetchPlayListDetailUseCase-any FetchPlayListDetailUseCase"] = { self.fetchPlayListDetailUseCase as Any }
        localTable["createPlayListUseCase-any CreatePlayListUseCase"] = { self.createPlayListUseCase as Any }
        localTable["editPlayListUseCase-any EditPlayListUseCase"] = { self.editPlayListUseCase as Any }
        localTable["editPlayListNameUseCase-any EditPlayListNameUseCase"] = { self.editPlayListNameUseCase as Any }
        localTable["deletePlayListUseCase-any DeletePlayListUseCase"] = { self.deletePlayListUseCase as Any }
        localTable["loadPlayListUseCase-any LoadPlayListUseCase"] = { self.loadPlayListUseCase as Any }
        localTable["artistComponent-ArtistComponent"] = { self.artistComponent as Any }
        localTable["remoteArtistDataSource-RemoteArtistDataSourceImpl"] = { self.remoteArtistDataSource as Any }
        localTable["artistRepository-any ArtistRepository"] = { self.artistRepository as Any }
        localTable["fetchArtistListUseCase-any FetchArtistListUseCase"] = { self.fetchArtistListUseCase as Any }
        localTable["artistDetailComponent-ArtistDetailComponent"] = { self.artistDetailComponent as Any }
        localTable["fetchArtistSongListUseCase-any FetchArtistSongListUseCase"] = { self.fetchArtistSongListUseCase as Any }
        localTable["artistMusicComponent-ArtistMusicComponent"] = { self.artistMusicComponent as Any }
        localTable["artistMusicContentComponent-ArtistMusicContentComponent"] = { self.artistMusicContentComponent as Any }
        localTable["profilePopComponent-ProfilePopComponent"] = { self.profilePopComponent as Any }
        localTable["favoriteComponent-FavoriteComponent"] = { self.favoriteComponent as Any }
        localTable["remoteUserDataSource-any RemoteUserDataSource"] = { self.remoteUserDataSource as Any }
        localTable["userRepository-any UserRepository"] = { self.userRepository as Any }
        localTable["fetchProfileListUseCase-any FetchProfileListUseCase"] = { self.fetchProfileListUseCase as Any }
        localTable["setProfileUseCase-any SetProfileUseCase"] = { self.setProfileUseCase as Any }
        localTable["setUserNameUseCase-any SetUserNameUseCase"] = { self.setUserNameUseCase as Any }
        localTable["fetchPlayListUseCase-any FetchPlayListUseCase"] = { self.fetchPlayListUseCase as Any }
        localTable["fetchFavoriteSongsUseCase-any FetchFavoriteSongsUseCase"] = { self.fetchFavoriteSongsUseCase as Any }
        localTable["editFavoriteSongsOrderUseCase-any EditFavoriteSongsOrderUseCase"] = { self.editFavoriteSongsOrderUseCase as Any }
        localTable["editPlayListOrderUseCase-any EditPlayListOrderUseCase"] = { self.editPlayListOrderUseCase as Any }
        localTable["mainContainerComponent-MainContainerComponent"] = { self.mainContainerComponent as Any }
        localTable["bottomTabBarComponent-BottomTabBarComponent"] = { self.bottomTabBarComponent as Any }
        localTable["mainTabBarComponent-MainTabBarComponent"] = { self.mainTabBarComponent as Any }
        localTable["playerComponent-PlayerComponent"] = { self.playerComponent as Any }
        localTable["playlistComponent-PlaylistComponent"] = { self.playlistComponent as Any }
        localTable["qnaComponent-QnaComponent"] = { self.qnaComponent as Any }
        localTable["qnaContentComponent-QnaContentComponent"] = { self.qnaContentComponent as Any }
        localTable["remoteQnaDataSource-any RemoteQnaDataSource"] = { self.remoteQnaDataSource as Any }
        localTable["qnaRepository-any QnaRepository"] = { self.qnaRepository as Any }
        localTable["fetchQnaCategoriesUseCase-any FetchQnaCategoriesUseCase"] = { self.fetchQnaCategoriesUseCase as Any }
        localTable["fetchQnaUseCase-any FetchQnaUseCase"] = { self.fetchQnaUseCase as Any }
        localTable["chartComponent-ChartComponent"] = { self.chartComponent as Any }
        localTable["chartContentComponent-ChartContentComponent"] = { self.chartContentComponent as Any }
        localTable["remoteChartDataSource-any RemoteChartDataSource"] = { self.remoteChartDataSource as Any }
        localTable["chartRepository-any ChartRepository"] = { self.chartRepository as Any }
        localTable["fetchChartRankingUseCase-any FetchChartRankingUseCase"] = { self.fetchChartRankingUseCase as Any }
        localTable["fetchChartUpdateTimeUseCase-any FetchChartUpdateTimeUseCase"] = { self.fetchChartUpdateTimeUseCase as Any }
    }
}
extension ArtistComponent: Registration {
    public func registerItems() {
        keyPathToName[\ArtistDependency.fetchArtistListUseCase] = "fetchArtistListUseCase-any FetchArtistListUseCase"
        keyPathToName[\ArtistDependency.artistDetailComponent] = "artistDetailComponent-ArtistDetailComponent"
    }
}
extension ArtistDetailComponent: Registration {
    public func registerItems() {
        keyPathToName[\ArtistDetailDependency.artistMusicComponent] = "artistMusicComponent-ArtistMusicComponent"
    }
}
extension ArtistMusicContentComponent: Registration {
    public func registerItems() {
        keyPathToName[\ArtistMusicContentDependency.fetchArtistSongListUseCase] = "fetchArtistSongListUseCase-any FetchArtistSongListUseCase"
    }
}
extension ArtistMusicComponent: Registration {
    public func registerItems() {
        keyPathToName[\ArtistMusicDependency.artistMusicContentComponent] = "artistMusicContentComponent-ArtistMusicContentComponent"
    }
}
extension PlaylistComponent: Registration {
    public func registerItems() {

    }
}
extension PlayerComponent: Registration {
    public func registerItems() {
        keyPathToName[\PlayerDependency.fetchLyricsUseCase] = "fetchLyricsUseCase-any FetchLyricsUseCase"
        keyPathToName[\PlayerDependency.playlistComponent] = "playlistComponent-PlaylistComponent"
    }
}
extension MainTabBarComponent: Registration {
    public func registerItems() {
        keyPathToName[\MainTabBarDependency.chartComponent] = "chartComponent-ChartComponent"
        keyPathToName[\MainTabBarDependency.searchComponent] = "searchComponent-SearchComponent"
        keyPathToName[\MainTabBarDependency.artistComponent] = "artistComponent-ArtistComponent"
        keyPathToName[\MainTabBarDependency.storageComponent] = "storageComponent-StorageComponent"
    }
}
extension BottomTabBarComponent: Registration {
    public func registerItems() {

    }
}
extension MainContainerComponent: Registration {
    public func registerItems() {
        keyPathToName[\MainContainerDependency.bottomTabBarComponent] = "bottomTabBarComponent-BottomTabBarComponent"
        keyPathToName[\MainContainerDependency.mainTabBarComponent] = "mainTabBarComponent-MainTabBarComponent"
        keyPathToName[\MainContainerDependency.playerComponent] = "playerComponent-PlayerComponent"
    }
}
extension ChartComponent: Registration {
    public func registerItems() {
        keyPathToName[\ChartDependency.chartContentComponent] = "chartContentComponent-ChartContentComponent"
    }
}
extension ChartContentComponent: Registration {
    public func registerItems() {
        keyPathToName[\ChartContentDependency.fetchChartRankingUseCase] = "fetchChartRankingUseCase-any FetchChartRankingUseCase"
        keyPathToName[\ChartContentDependency.fetchChartUpdateTimeUseCase] = "fetchChartUpdateTimeUseCase-any FetchChartUpdateTimeUseCase"
    }
}
extension StorageComponent: Registration {
    public func registerItems() {
        keyPathToName[\StorageDependency.signInComponent] = "signInComponent-SignInComponent"
        keyPathToName[\StorageDependency.afterLoginComponent] = "afterLoginComponent-AfterLoginComponent"
    }
}
extension MyPlayListComponent: Registration {
    public func registerItems() {
        keyPathToName[\MyPlayListDependency.multiPurposePopComponent] = "multiPurposePopComponent-MultiPurposePopComponent"
        keyPathToName[\MyPlayListDependency.playListDetailComponent] = "playListDetailComponent-PlayListDetailComponent"
        keyPathToName[\MyPlayListDependency.fetchPlayListUseCase] = "fetchPlayListUseCase-any FetchPlayListUseCase"
        keyPathToName[\MyPlayListDependency.editPlayListOrderUseCase] = "editPlayListOrderUseCase-any EditPlayListOrderUseCase"
    }
}
extension AfterLoginComponent: Registration {
    public func registerItems() {
        keyPathToName[\AfterLoginDependency.fetchUserInfoUseCase] = "fetchUserInfoUseCase-any FetchUserInfoUseCase"
        keyPathToName[\AfterLoginDependency.requestComponent] = "requestComponent-RequestComponent"
        keyPathToName[\AfterLoginDependency.profilePopComponent] = "profilePopComponent-ProfilePopComponent"
        keyPathToName[\AfterLoginDependency.myPlayListComponent] = "myPlayListComponent-MyPlayListComponent"
        keyPathToName[\AfterLoginDependency.multiPurposePopComponent] = "multiPurposePopComponent-MultiPurposePopComponent"
        keyPathToName[\AfterLoginDependency.favoriteComponent] = "favoriteComponent-FavoriteComponent"
    }
}
extension FavoriteComponent: Registration {
    public func registerItems() {
        keyPathToName[\FavoriteDependency.fetchFavoriteSongsUseCase] = "fetchFavoriteSongsUseCase-any FetchFavoriteSongsUseCase"
        keyPathToName[\FavoriteDependency.editFavoriteSongsOrderUseCase] = "editFavoriteSongsOrderUseCase-any EditFavoriteSongsOrderUseCase"
    }
}
extension QnaComponent: Registration {
    public func registerItems() {
        keyPathToName[\QnaDependency.qnaContentComponent] = "qnaContentComponent-QnaContentComponent"
        keyPathToName[\QnaDependency.fetchQnaCategoriesUseCase] = "fetchQnaCategoriesUseCase-any FetchQnaCategoriesUseCase"
        keyPathToName[\QnaDependency.fetchQnaUseCase] = "fetchQnaUseCase-any FetchQnaUseCase"
    }
}
extension RequestComponent: Registration {
    public func registerItems() {
        keyPathToName[\RequestDependency.withdrawUserInfoUseCase] = "withdrawUserInfoUseCase-any WithdrawUserInfoUseCase"
        keyPathToName[\RequestDependency.qnaComponent] = "qnaComponent-QnaComponent"
    }
}
extension QnaContentComponent: Registration {
    public func registerItems() {

    }
}
extension RootComponent: Registration {
    public func registerItems() {
        keyPathToName[\RootDependency.mainContainerComponent] = "mainContainerComponent-MainContainerComponent"
    }
}
extension SignInComponent: Registration {
    public func registerItems() {
        keyPathToName[\SignInDependency.fetchTokenUseCase] = "fetchTokenUseCase-any FetchTokenUseCase"
        keyPathToName[\SignInDependency.fetchNaverUserInfoUseCase] = "fetchNaverUserInfoUseCase-any FetchNaverUserInfoUseCase"
        keyPathToName[\SignInDependency.fetchUserInfoUseCase] = "fetchUserInfoUseCase-any FetchUserInfoUseCase"
    }
}
extension AfterSearchComponent: Registration {
    public func registerItems() {
        keyPathToName[\AfterSearchDependency.afterSearchContentComponent] = "afterSearchContentComponent-AfterSearchContentComponent"
        keyPathToName[\AfterSearchDependency.fetchSearchSongUseCase] = "fetchSearchSongUseCase-any FetchSearchSongUseCase"
    }
}
extension AfterSearchContentComponent: Registration {
    public func registerItems() {

    }
}
extension SearchComponent: Registration {
    public func registerItems() {
        keyPathToName[\SearchDependency.beforeSearchComponent] = "beforeSearchComponent-BeforeSearchComponent"
        keyPathToName[\SearchDependency.afterSearchComponent] = "afterSearchComponent-AfterSearchComponent"
    }
}
extension BeforeSearchComponent: Registration {
    public func registerItems() {
        keyPathToName[\BeforeSearchDependency.playListDetailComponent] = "playListDetailComponent-PlayListDetailComponent"
        keyPathToName[\BeforeSearchDependency.fetchRecommendPlayListUseCase] = "fetchRecommendPlayListUseCase-any FetchRecommendPlayListUseCase"
    }
}
extension MultiPurposePopComponent: Registration {
    public func registerItems() {
        keyPathToName[\MultiPurposePopDependency.createPlayListUseCase] = "createPlayListUseCase-any CreatePlayListUseCase"
        keyPathToName[\MultiPurposePopDependency.loadPlayListUseCase] = "loadPlayListUseCase-any LoadPlayListUseCase"
        keyPathToName[\MultiPurposePopDependency.setUserNameUseCase] = "setUserNameUseCase-any SetUserNameUseCase"
        keyPathToName[\MultiPurposePopDependency.editPlayListNameUseCase] = "editPlayListNameUseCase-any EditPlayListNameUseCase"
    }
}
extension PlayListDetailComponent: Registration {
    public func registerItems() {
        keyPathToName[\PlayListDetailDependency.fetchPlayListDetailUseCase] = "fetchPlayListDetailUseCase-any FetchPlayListDetailUseCase"
        keyPathToName[\PlayListDetailDependency.editPlayListUseCase] = "editPlayListUseCase-any EditPlayListUseCase"
        keyPathToName[\PlayListDetailDependency.multiPurposePopComponent] = "multiPurposePopComponent-MultiPurposePopComponent"
    }
}
extension ProfilePopComponent: Registration {
    public func registerItems() {
        keyPathToName[\ProfilePopDependency.fetchProfileListUseCase] = "fetchProfileListUseCase-any FetchProfileListUseCase"
        keyPathToName[\ProfilePopDependency.setProfileUseCase] = "setProfileUseCase-any SetProfileUseCase"
    }
}


#endif

private func factoryEmptyDependencyProvider(_ component: NeedleFoundation.Scope) -> AnyObject {
    return EmptyDependencyProvider(component: component)
}

// MARK: - Registration
private func registerProviderFactory(_ componentPath: String, _ factory: @escaping (NeedleFoundation.Scope) -> AnyObject) {
    __DependencyProviderRegistry.instance.registerDependencyProviderFactory(for: componentPath, factory)
}

#if !NEEDLE_DYNAMIC

@inline(never) private func register1() {
    registerProviderFactory("^->AppComponent", factoryEmptyDependencyProvider)
    registerProviderFactory("^->AppComponent->ArtistComponent", factorye0c5444f5894148bdd93f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->ArtistDetailComponent", factory35314797fadaf164ece6f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->ArtistMusicContentComponent", factory8b6ffa46033e2529b5daf47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->ArtistMusicComponent", factory382e7f8466df35a3f1d9f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->PlaylistComponent", factory3a0a6eb1061d8d5a2defe3b0c44298fc1c149afb)
    registerProviderFactory("^->AppComponent->PlayerComponent", factorybc7f802f601dd5913533f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->MainTabBarComponent", factorye547a52b3fce5887c8c7f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->BottomTabBarComponent", factoryd34fa9e493604a6295bde3b0c44298fc1c149afb)
    registerProviderFactory("^->AppComponent->MainContainerComponent", factory8e19f48d5d573d3ea539f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->ChartComponent", factoryeac6a4df54bbd391d31bf47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->ChartContentComponent", factoryc9a137630ce76907f36ff47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->StorageComponent", factory2415399d25299b97b98bf47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->MyPlayListComponent", factory51a57a92f76af93a9ec2f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->AfterLoginComponent", factory6cc9c8141e04494113b8f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->FavoriteComponent", factory8e4acb90bd0d9b48604af47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->QnaComponent", factory49a98666675cb7a82038f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->RequestComponent", factory13954fb3ec537bab80bcf47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->QnaContentComponent", factory1501f7005831c8411229e3b0c44298fc1c149afb)
    registerProviderFactory("^->AppComponent->RootComponent", factory264bfc4d4cb6b0629b40f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->SignInComponent", factoryda2925fd76da866a652af47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->AfterSearchComponent", factoryeb2da679e35e2c4fb9a5f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->AfterSearchContentComponent", factorycaaccdf52467bfa87f73e3b0c44298fc1c149afb)
    registerProviderFactory("^->AppComponent->SearchComponent", factorye3d049458b2ccbbcb3b6f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->BeforeSearchComponent", factory9bb852337d5550979293f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->MultiPurposePopComponent", factory972fcba2860fcb8ad7b8f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->PlayListDetailComponent", factory9e077ee814ce180ea399f47b58f8f304c97af4d5)
    registerProviderFactory("^->AppComponent->ProfilePopComponent", factorybd14b11ccce6dac94a24f47b58f8f304c97af4d5)
}
#endif

public func registerProviderFactories() {
#if !NEEDLE_DYNAMIC
    register1()
#endif
}
