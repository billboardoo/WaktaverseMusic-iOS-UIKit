import BaseFeature
import ChartDomainInterface
import ChartFeatureInterface
import DesignSystem
import LogManager
import NVActivityIndicatorView
import PlayListDomainInterface
import PlaylistFeatureInterface
import RxCocoa
import RxSwift
import SnapKit
import SongsDomainInterface
import Then
import UIKit
import Utility

public final class HomeViewController: BaseViewController, ViewControllerFromStoryBoard, EqualHandleTappedType {
    @IBOutlet weak var topSpaceConstraint: NSLayoutConstraint!
    @IBOutlet weak var scrollView: UIScrollView!
    @IBOutlet weak var stackView: UIStackView!
    @IBOutlet weak var activityIndicator: NVActivityIndicatorView!

    /// 왁뮤차트 TOP100
    @IBOutlet weak var topCircleImageView: UIImageView!
    @IBOutlet weak var chartContentView: UIView!
    @IBOutlet weak var chartBorderView: UIView!
    @IBOutlet weak var chartTitleLabel: UILabel!
    @IBOutlet weak var chartArrowImageView: UIImageView!
    @IBOutlet weak var chartAllListenButton: UIButton!
    @IBOutlet weak var chartMoreButton: UIButton!
    @IBOutlet weak var tableView: UITableView!

    /// 최신음악
    @IBOutlet weak var latestSongLabel: UILabel!
    @IBOutlet weak var latestArrowImageView: UIImageView!
    @IBOutlet weak var latestSongsMoveButton: UIButton!
    @IBOutlet weak var latestSongsPlayButton: UIButton!
    @IBOutlet weak var collectionView: UICollectionView!
    @IBOutlet weak var latestSongEmptyLabel: UILabel!

    private let blurImageView = UIImageView().then {
        $0.layer.cornerRadius = 12
        $0.clipsToBounds = true
        $0.image = DesignSystemAsset.Home.blurBg.image
        $0.contentMode = .scaleAspectFill
    }

    private let glassmorphismView = GlassmorphismView().then {
        $0.setCornerRadius(12)
        $0.setTheme(theme: .light)
        $0.setDistance(100)
        $0.layer.cornerRadius = 12
        $0.clipsToBounds = true
    }

    private var refreshControl = UIRefreshControl()
    var chartFactory: ChartFactory!
    var playlistDetailFactory: PlaylistDetailFactory!
    var newSongsComponent: NewSongsComponent!
    var recommendViewHeightConstraint: NSLayoutConstraint?

    var viewModel: HomeViewModel!
    private lazy var input = HomeViewModel.Input()
    private lazy var output = viewModel.transform(from: input)
    var disposeBag = DisposeBag()

    override public func viewDidLoad() {
        super.viewDidLoad()
        configureUI()
        configureBlurUI()
        outputBind()
        inputBind()
    }

    override public func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        LogManager.analytics(CommonAnalyticsLog.viewPage(pageName: .home))
        navigationController?.interactivePopGestureRecognizer?.delegate = self
    }

    override public func viewDidDisappear(_ animated: Bool) {
        super.viewDidDisappear(animated)
        navigationController?.interactivePopGestureRecognizer?.delegate = nil
    }

    public static func viewController(
        viewModel: HomeViewModel,
        playlistDetailFactory: PlaylistDetailFactory,
        newSongsComponent: NewSongsComponent,
        chartFactory: ChartFactory
    ) -> HomeViewController {
        let viewController = HomeViewController.viewController(storyBoardName: "Home", bundle: Bundle.module)
        viewController.viewModel = viewModel
        viewController.playlistDetailFactory = playlistDetailFactory
        viewController.newSongsComponent = newSongsComponent
        viewController.chartFactory = chartFactory
        return viewController
    }
}

extension HomeViewController {
    private func inputBind() {
        input.fetchHomeUseCase.onNext(())

        chartMoreButton.rx.tap
            .bind(with: self, onNext: { owner, _ in
                LogManager.analytics(HomeAnalyticsLog.clickChartTop100MusicsTitleButton)
                let viewController = owner.chartFactory.makeView()
                owner.navigationController?.pushViewController(viewController, animated: true)
            })
            .disposed(by: disposeBag)

        chartAllListenButton.rx.tap
            .bind(to: input.chartAllListenTapped)
            .disposed(by: disposeBag)

        latestSongsMoveButton.rx.tap
            .withUnretained(self)
            .subscribe(onNext: { owner, _ in
                let viewController = owner.newSongsComponent.makeView()
                owner.navigationController?.pushViewController(viewController, animated: true)
            })
            .disposed(by: disposeBag)

        latestSongsPlayButton.rx.tap
            .bind(to: input.newSongsAllListenTapped)
            .disposed(by: disposeBag)

        refreshControl.rx
            .controlEvent(.valueChanged)
            .bind(to: input.refreshPulled)
            .disposed(by: disposeBag)

        tableView.rx.itemSelected
            .withLatestFrom(output.chartDataSource) { ($0, $1) }
            .map { SongEntity(
                id: $0.1[$0.0.row].id,
                title: $0.1[$0.0.row].title,
                artist: $0.1[$0.0.row].artist,
                remix: "",
                reaction: "",
                views: $0.1[$0.0.row].views,
                last: $0.1[$0.0.row].last,
                date: $0.1[$0.0.row].date
            )
            }
            .subscribe(onNext: { song in
                LogManager.analytics(HomeAnalyticsLog.clickMusicItem(location: .homeTop100))
                PlayState.shared.loadAndAppendSongsToPlaylist([song])
            })
            .disposed(by: disposeBag)

        collectionView.rx.itemSelected
            .withLatestFrom(output.newSongDataSource) { ($0, $1) }
            .map { SongEntity(
                id: $0.1[$0.0.row].id,
                title: $0.1[$0.0.row].title,
                artist: $0.1[$0.0.row].artist,
                remix: $0.1[$0.0.row].remix,
                reaction: $0.1[$0.0.row].reaction,
                views: $0.1[$0.0.row].views,
                last: $0.1[$0.0.row].last,
                date: "\($0.1[$0.0.row].date)"
            )
            }
            .subscribe(onNext: { song in
                LogManager.analytics(HomeAnalyticsLog.clickMusicItem(location: .homeRecent))
                PlayState.shared.loadAndAppendSongsToPlaylist([song])
            })
            .disposed(by: disposeBag)
    }

    private func outputBind() {
        tableView.rx
            .setDelegate(self)
            .disposed(by: disposeBag)

        collectionView.rx
            .setDelegate(self)
            .disposed(by: disposeBag)

        output.chartDataSource
            .skip(1)
            .filter { $0.count >= 5 }
            .map { Array($0[0 ..< 5]) }
            .do(onNext: { [weak self] _ in
                self?.activityIndicator.stopAnimating()
            })
            .bind(to: tableView.rx.items) { tableView, index, model -> UITableViewCell in
                let indexPath: IndexPath = IndexPath(row: index, section: 0)
                guard let cell = tableView
                    .dequeueReusableCell(
                        withIdentifier: "HomeChartCell",
                        for: indexPath
                    ) as? HomeChartCell else {
                    return UITableViewCell()
                }
                cell.update(model: model, index: indexPath.row)
                return cell
            }
            .disposed(by: disposeBag)

        output.newSongDataSource
            .skip(1)
            .do(onNext: { [weak self] model in
                self?.collectionView.contentOffset = .zero
                self?.refreshControl.endRefreshing()
                self?.activityIndicator.stopAnimating()
                self?.latestSongEmptyLabel.isHidden = !model.isEmpty
            })
            .bind(to: collectionView.rx.items) { collectionView, index, model -> UICollectionViewCell in
                let indexPath = IndexPath(item: index, section: 0)
                guard let cell = collectionView.dequeueReusableCell(
                    withReuseIdentifier: "HomeNewSongCell",
                    for: indexPath
                ) as? HomeNewSongCell else {
                    return UICollectionViewCell()
                }
                cell.update(model: model)
                return cell
            }.disposed(by: disposeBag)

        output.playListDataSource
            .skip(1)
            .filter { !$0.isEmpty }
            .observe(on: MainScheduler.instance)
            .subscribe(onNext: { [weak self] model in
                guard let `self` = self else { return }
                let subviews: [UIView] = self.stackView.arrangedSubviews.filter { $0 is RecommendPlayListView }
                    .compactMap { $0 }
                let height: CGFloat = RecommendPlayListView.getViewHeight(model: model)

                if subviews.isEmpty {
                    let recommendView = RecommendPlayListView(
                        frame: CGRect(
                            x: 0,
                            y: 0,
                            width: APP_WIDTH(),
                            height: height
                        )
                    )
                    recommendView.dataSource = model
                    recommendView.delegate = self
                    let constraint = recommendView.heightAnchor.constraint(equalToConstant: height)
                    constraint.isActive = true
                    self.stackView.addArrangedSubview(recommendView)
                    self.recommendViewHeightConstraint = constraint

                    let bottomSpace = UIView(frame: CGRect(x: 0, y: 0, width: APP_WIDTH(), height: 56))
                    bottomSpace.heightAnchor.constraint(equalToConstant: bottomSpace.frame.height).isActive = true
                    self.stackView.addArrangedSubview(bottomSpace)

                } else {
                    guard let recommendView = subviews.first as? RecommendPlayListView else { return }
                    self.recommendViewHeightConstraint?.constant = height
                    recommendView.dataSource = model
                }
            })
            .disposed(by: disposeBag)
    }

    private func configureBlurUI() {
        [blurImageView, glassmorphismView].forEach {
            chartContentView.insertSubview($0, at: 0)
            $0.snp.makeConstraints {
                $0.left.equalToSuperview().offset(20)
                $0.right.equalToSuperview().offset(-20)
                $0.top.bottom.equalToSuperview()
            }
        }
    }

    private func configureUI() {
        activityIndicator.type = .circleStrokeSpin
        activityIndicator.color = DesignSystemAsset.PrimaryColor.point.color
        activityIndicator.startAnimating()
        view.backgroundColor = DesignSystemAsset.BlueGrayColor.gray100.color
        topCircleImageView.image = DesignSystemAsset.Home.gradationBg.image

        chartBorderView.layer.cornerRadius = 12
        chartBorderView.layer.borderWidth = 1
        chartBorderView.layer.borderColor = DesignSystemAsset.BlueGrayColor.gray25.color.cgColor

        let mainTitleLabelAttributedString = NSMutableAttributedString(
            string: "왁뮤차트 TOP100",
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.bold.font(size: 16),
                .foregroundColor: DesignSystemAsset.BlueGrayColor.gray900.color,
                .kern: -0.5
            ]
        )
        chartTitleLabel.attributedText = mainTitleLabelAttributedString

        let mainTitleAllButtonAttributedString = NSMutableAttributedString(
            string: "전체듣기",
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.medium.font(size: 14),
                .foregroundColor: DesignSystemAsset.BlueGrayColor.gray25.color,
                .kern: -0.5
            ]
        )
        chartAllListenButton.setAttributedTitle(mainTitleAllButtonAttributedString, for: .normal)
        chartArrowImageView.image = DesignSystemAsset.Home.homeArrowRight.image

        let latestSongAttributedString = NSMutableAttributedString(
            string: "최신 음악",
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.bold.font(size: 16),
                .foregroundColor: DesignSystemAsset.BlueGrayColor.gray900.color,
                .kern: -0.5
            ]
        )
        latestSongLabel.attributedText = latestSongAttributedString
        latestArrowImageView.image = DesignSystemAsset.Home.homeArrowRight.image

        let latestSongPlayAttributedString = NSMutableAttributedString(
            string: "전체듣기",
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.medium.font(size: 14),
                .foregroundColor: DesignSystemAsset.BlueGrayColor.gray900.color.withAlphaComponent(0.6),
                .kern: -0.5
            ]
        )
        latestSongsPlayButton.setAttributedTitle(latestSongPlayAttributedString, for: .normal)

        latestSongEmptyLabel.isHidden = true
        latestSongEmptyLabel.text = "현재 집계된 음악이 없습니다."
        latestSongEmptyLabel.textColor = DesignSystemAsset.BlueGrayColor.gray900.color
        latestSongEmptyLabel.font = DesignSystemFontFamily.Pretendard.medium.font(size: 14)
        latestSongEmptyLabel.setTextWithAttributes(kernValue: -0.5)
        latestSongEmptyLabel.textAlignment = .center
        scrollView.refreshControl = refreshControl
        scrollView.delegate = self
    }
}

extension HomeViewController: UIScrollViewDelegate {
    public func scrollViewDidScroll(_ scrollView: UIScrollView) {
        guard collectionView != scrollView else { return }
        let offsetY: CGFloat = scrollView.contentOffset.y + STATUS_BAR_HEGHIT()
        let standard: CGFloat = offsetY / topCircleImageView.frame.height
        blurImageView.alpha = 1.0 - standard
        glassmorphismView.alpha = min(1.0, standard + 0.8)
    }
}

extension HomeViewController: UITableViewDelegate {
    public func tableView(_ tableView: UITableView, heightForRowAt indexPath: IndexPath) -> CGFloat {
        return 58
    }
}

extension HomeViewController: UICollectionViewDelegate, UICollectionViewDelegateFlowLayout {
    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        sizeForItemAt indexPath: IndexPath
    ) -> CGSize {
        return CGSize(width: 144.0, height: 131.0)
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        insetForSectionAt section: Int
    ) -> UIEdgeInsets {
        return UIEdgeInsets(top: 0, left: 20.0, bottom: 0, right: 20.0)
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        minimumLineSpacingForSectionAt section: Int
    ) -> CGFloat {
        return 8.0
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        minimumInteritemSpacingForSectionAt section: Int
    ) -> CGFloat {
        return 8.0
    }
}

extension HomeViewController: RecommendPlayListViewDelegate {
    public func itemSelected(model: RecommendPlayListEntity) {
        LogManager.analytics(CommonAnalyticsLog.clickPlaylistItem(location: .home))
        let playListDetailVc = playlistDetailFactory.makeView(id: model.key, isCustom: false)
        self.navigationController?.pushViewController(playListDetailVc, animated: true)
    }
}

public extension HomeViewController {
    func equalHandleTapped() {
        let viewControllersCount: Int = self.navigationController?.viewControllers.count ?? 0
        if viewControllersCount > 1 {
            self.navigationController?.popToRootViewController(animated: true)
        } else {
            scrollView.setContentOffset(CGPoint(x: 0, y: -STATUS_BAR_HEGHIT()), animated: true)
        }
    }
}

extension HomeViewController: UIGestureRecognizerDelegate {
    public func gestureRecognizerShouldBegin(_ gestureRecognizer: UIGestureRecognizer) -> Bool {
        return false
    }
}
