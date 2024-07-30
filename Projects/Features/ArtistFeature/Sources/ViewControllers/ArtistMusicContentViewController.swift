import ArtistDomainInterface
import BaseFeature
import BaseFeatureInterface
import DesignSystem
import Localization
import LogManager
import NVActivityIndicatorView
import RxCocoa
import RxSwift
import SongsDomainInterface
import UIKit
import Utility

public final class ArtistMusicContentViewController:
    BaseViewController, ViewControllerFromStoryBoard, SongCartViewType {
    @IBOutlet weak var tableView: UITableView!
    @IBOutlet weak var activityIndidator: NVActivityIndicatorView!
    @IBOutlet weak var songCartOnView: UIView!

    public var songCartView: SongCartView!
    public var bottomSheetView: BottomSheetView!
    private var containSongsFactory: ContainSongsFactory!
    private var songDetailPresenter: SongDetailPresentable!

    private var viewModel: ArtistMusicContentViewModel!
    lazy var input = ArtistMusicContentViewModel.Input()
    lazy var output = viewModel.transform(from: input)
    private let disposeBag = DisposeBag()

    deinit {
        LogManager.printDebug("\(Self.self) Deinit")
    }

    override public func viewDidLoad() {
        super.viewDidLoad()
        configureUI()
        inputBind()
        outputBind()
    }

    override public func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        self.input.allSongSelected.onNext(false)
    }

    public static func viewController(
        viewModel: ArtistMusicContentViewModel,
        containSongsFactory: ContainSongsFactory,
        songDetailPresenter: SongDetailPresentable
    ) -> ArtistMusicContentViewController {
        let viewController = ArtistMusicContentViewController.viewController(
            storyBoardName: "Artist",
            bundle: Bundle.module
        )
        viewController.viewModel = viewModel
        viewController.containSongsFactory = containSongsFactory
        viewController.songDetailPresenter = songDetailPresenter
        return viewController
    }
}

private extension ArtistMusicContentViewController {
    func inputBind() {
        tableView.rx
            .setDelegate(self)
            .disposed(by: disposeBag)

        tableView.rx.willDisplayCell
            .map { $1 }
            .withLatestFrom(
                output.dataSource,
                resultSelector: { indexPath, datasource -> (IndexPath, [ArtistSongListEntity]) in
                    return (indexPath, datasource)
                }
            )
            .filter { indexPath, datasources -> Bool in
                return indexPath.item == datasources.count - 1
            }
            .withLatestFrom(output.canLoadMore)
            .filter { $0 }
            .map { _ in return () }
            .bind(to: rx.loadMore)
            .disposed(by: disposeBag)

        tableView.rx.itemSelected
            .map { $0.row }
            .bind(to: input.songTapped)
            .disposed(by: disposeBag)
    }

    func outputBind() {
        output.dataSource
            .skip(1)
            .withLatestFrom(output.indexOfSelectedSongs) { ($0, $1) }
            .do(onNext: { [weak self] dataSource, songs in
                guard let self = self else { return }
                let height = self.tableView.frame.height / 3 * 2
                let warningView = WarningView(frame: CGRect(x: 0, y: 0, width: APP_WIDTH(), height: height))
                warningView.text = "아티스트 곡이 없습니다."
                self.tableView.tableFooterView = dataSource.isEmpty ?
                    warningView : UIView(frame: CGRect(x: 0, y: 0, width: APP_WIDTH(), height: PLAYER_HEIGHT()))
                self.activityIndidator.stopAnimating()
                guard let songCart = self.songCartView else { return }
                songCart.updateAllSelect(isAll: songs.count == dataSource.count)
            })
            .map { $0.0 }
            .bind(to: tableView.rx.items) { [weak self] tableView, index, model -> UITableViewCell in
                guard let self = self,
                      let cell = tableView.dequeueReusableCell(
                          withIdentifier: "ArtistMusicCell",
                          for: IndexPath(row: index, section: 0)
                      ) as? ArtistMusicCell else {
                    return UITableViewCell()
                }
                cell.delegate = self
                cell.update(model: model)
                return cell
            }
            .disposed(by: disposeBag)

        output.indexOfSelectedSongs
            .skip(1)
            .withLatestFrom(output.dataSource) { ($0, $1) }
            .subscribe(onNext: { [weak self] songs, dataSource in
                guard let self = self else { return }

                switch songs.isEmpty {
                case true:
                    UIView.animate(withDuration: 0.5) {
                        self.songCartOnView.alpha = .zero
                    }
                    self.hideSongCart()
                case false:
                    self.songCartOnView.alpha = 1.0
                    self.showSongCart(
                        in: self.songCartOnView,
                        type: .artistSong,
                        selectedSongCount: songs.count,
                        totalSongCount: dataSource.count,
                        useBottomSpace: false
                    )
                    self.songCartView?.delegate = self
                }
            })
            .disposed(by: disposeBag)

        output.showToast
            .bind(with: self) { owner, message in
                owner.showToast(text: message, options: [.tabBar, .songCart])
            }
            .disposed(by: disposeBag)
    }

    func configureUI() {
        activityIndidator.color = DesignSystemAsset.PrimaryColor.point.color
        activityIndidator.type = .circleStrokeSpin
        activityIndidator.startAnimating()
        tableView.backgroundColor = .clear
        songCartOnView.alpha = .zero
    }
}

extension ArtistMusicContentViewController: ArtistMusicCellDelegate {
    func tappedThumbnail(id: String) {
        songDetailPresenter.present(id: id)
    }
}

extension ArtistMusicContentViewController: SongCartViewDelegate {
    public func buttonTapped(type: SongCartSelectType) {
        let limit: Int = 50
        let songs: [SongEntity] = output.songEntityOfSelectedSongs.value

        switch type {
        case let .allSelect(flag):
            input.allSongSelected.onNext(flag)

        case .addSong:
            guard songs.count <= limit else {
                output.showToast.onNext(LocalizationStrings.overFlowContainWarning(songs.count - limit))
                return
            }
            let songIds: [String] = songs.map { $0.id }
            let viewController = containSongsFactory.makeView(songs: songIds)
            viewController.modalPresentationStyle = .overFullScreen
            self.present(viewController, animated: true) {
                self.input.allSongSelected.onNext(false)
            }

        case .addPlayList:
            guard songs.count <= limit else {
                output.showToast.onNext(LocalizationStrings.overFlowAddPlaylistWarning(songs.count - limit))
                return
            }
            PlayState.shared.appendSongsToPlaylist(songs)
            input.allSongSelected.onNext(false)

        case .play:
            guard songs.count <= limit else {
                output.showToast.onNext(LocalizationStrings.overFlowPlayWarning(songs.count - limit))
                return
            }
            PlayState.shared.loadAndAppendSongsToPlaylist(songs)
            input.allSongSelected.onNext(false)
            WakmusicYoutubePlayer(ids: songs.map { $0.id }).play()

        default: return
        }
    }
}

extension ArtistMusicContentViewController: UITableViewDelegate {
    public func tableView(_ tableView: UITableView, heightForRowAt indexPath: IndexPath) -> CGFloat {
        return 60
    }

    public func tableView(_ tableView: UITableView, heightForHeaderInSection section: Int) -> CGFloat {
        return output.dataSource.value.isEmpty ? 0 : 80
    }

    public func tableView(_ tableView: UITableView, viewForHeaderInSection section: Int) -> UIView? {
        let view = ArtistPlayButtonGroupView(frame: CGRect(x: 0, y: 0, width: APP_WIDTH(), height: 80))
        view.delegate = self
        return output.dataSource.value.isEmpty ? nil : view
    }
}

extension ArtistMusicContentViewController: PlayButtonGroupViewDelegate {
    public func play(_ event: PlayEvent) {
        LogManager.analytics(
            ArtistAnalyticsLog.clickArtistPlayButton(
                type: event == .allPlay ? "all" : "random",
                artist: viewModel.model?.id ?? ""
            )
        )
        let songs: [SongEntity] = output.dataSource.value.map {
            return SongEntity(
                id: $0.songID,
                title: $0.title,
                artist: $0.artist,
                views: 0,
                date: $0.date
            )
        }
        switch event {
        case .allPlay:
            PlayState.shared.loadAndAppendSongsToPlaylist(songs)

        case .shufflePlay:
            PlayState.shared.loadAndAppendSongsToPlaylist(songs.shuffled())
        }
        input.allSongSelected.onNext(false)
    }
}

extension Reactive where Base: ArtistMusicContentViewController {
    var refresh: Binder<Void> {
        return Binder(base) { viewController, _ in
            viewController.input.pageID.accept(1)
        }
    }

    var loadMore: Binder<Void> {
        return Binder(base) { viewController, _ in
            let pageID = viewController.input.pageID.value
            viewController.input.pageID.accept(pageID + 1)
        }
    }
}

extension ArtistMusicContentViewController: UIScrollViewDelegate {
    public func scrollViewDidScroll(_ scrollView: UIScrollView) {
        guard self.output.dataSource.value.count >= 25 else { return }
        guard let parent = self.parent?.parent?.parent as? ArtistDetailViewController else { return }
        let type = self.viewModel.type
        let i = (type == .new) ? 0 : (type == .popular) ? 1 : 2
        parent.scrollViewDidScrollFromChild(scrollView: scrollView, i: i)
    }
}
