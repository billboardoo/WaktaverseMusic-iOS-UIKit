//
//  NoticeDetailViewController.swift
//  StorageFeature
//
//  Created by KTH on 2023/04/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import BaseFeature
import DesignSystem
import NoticeDomainInterface
import NVActivityIndicatorView
import RxCocoa
import RxDataSources
import RxSwift
import SafariServices
import UIKit
import Utility

typealias NoticeDetailSectionModel = SectionModel<FetchNoticeEntity, FetchNoticeEntity.Image>

public final class NoticeDetailViewController: UIViewController, ViewControllerFromStoryBoard {
    @IBOutlet weak var titleStringLabel: UILabel!
    @IBOutlet weak var closeButton: UIButton!
    @IBOutlet weak var collectionView: UICollectionView!
    @IBOutlet weak var indicator: NVActivityIndicatorView!

    private var viewModel: NoticeDetailViewModel!
    private let disposeBag = DisposeBag()

    deinit {
        DEBUG_LOG("❌ \(Self.self) Deinit")
    }

    override public func viewDidLoad() {
        super.viewDidLoad()
        configureUI()
        outputBind()
        inputBind()
    }

    public static func viewController(
        viewModel: NoticeDetailViewModel
    ) -> NoticeDetailViewController {
        let viewController = NoticeDetailViewController.viewController(storyBoardName: "Storage", bundle: Bundle.module)
        viewController.viewModel = viewModel
        return viewController
    }

    @IBAction func closeButtonAction(_ sender: Any) {
        dismiss(animated: true)
    }
}

private extension NoticeDetailViewController {
    func inputBind() {
        viewModel.input.fetchNoticeDetail.onNext(())

        collectionView.rx.itemSelected
            .withLatestFrom(viewModel.output.dataSource) { ($0, $1) }
            .map { $0.1[$0.0.section].items[$0.0.item] }
            .bind(with: self) { owner, model in
                guard !model.link.isEmpty,
                    let URL = URL(string: model.link) else { return }
                owner.present(SFSafariViewController(url: URL), animated: true)
            }
            .disposed(by: disposeBag)
    }

    func outputBind() {
        viewModel.output.dataSource
            .bind(to: collectionView.rx.items(dataSource: createDataSource()))
            .disposed(by: disposeBag)

        viewModel.output.imageSizes
            .skip(1)
            .subscribe(onNext: { [weak self] _ in
                self?.indicator.stopAnimating()
            })
            .disposed(by: disposeBag)
    }

    func createDataSource() -> RxCollectionViewSectionedReloadDataSource<NoticeDetailSectionModel> {
        let dataSource = RxCollectionViewSectionedReloadDataSource<NoticeDetailSectionModel>(
            configureCell: { _, collectionView, indexPath, item -> UICollectionViewCell in
                guard let cell = collectionView.dequeueReusableCell(
                    withReuseIdentifier: "NoticeCollectionViewCell",
                    for: indexPath
                ) as? NoticeCollectionViewCell else {
                    return UICollectionViewCell()
                }
                cell.update(model: item)
                return cell
            },
            configureSupplementaryView: { dataSource, collectionView, elementKind, indexPath -> UICollectionReusableView in
                switch elementKind {
                case UICollectionView.elementKindSectionHeader:
                    if let header = collectionView.dequeueReusableSupplementaryView(
                        ofKind: elementKind,
                        withReuseIdentifier: "NoticeDetailHeaderView",
                        for: indexPath
                    ) as? NoticeDetailHeaderView {
                        header.update(model: dataSource[indexPath.section].model)
                        return header

                    } else { return UICollectionReusableView() }

                default:
                    return UICollectionReusableView()
                }
            }
        )
        return dataSource
    }

    func configureUI() {
        self.view.backgroundColor = DesignSystemAsset.BlueGrayColor.gray100.color
        closeButton.setImage(DesignSystemAsset.Navigation.crossClose.image, for: .normal)

        let attributedString: NSAttributedString = NSAttributedString(
            string: "공지사항",
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.medium.font(size: 16),
                .foregroundColor: DesignSystemAsset.BlueGrayColor.gray900.color,
                .kern: -0.5
            ]
        )
        self.titleStringLabel.attributedText = attributedString

        collectionView.register(
            UINib(nibName: "NoticeCollectionViewCell", bundle: BaseFeatureResources.bundle),
            forCellWithReuseIdentifier: "NoticeCollectionViewCell"
        )
        collectionView.rx.setDelegate(self).disposed(by: disposeBag)
        collectionView.bounces = false

        self.indicator.type = .circleStrokeSpin
        self.indicator.color = DesignSystemAsset.PrimaryColor.point.color
        self.indicator.startAnimating()
    }
}

extension NoticeDetailViewController: UICollectionViewDelegate, UICollectionViewDelegateFlowLayout {
    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        sizeForItemAt indexPath: IndexPath
    ) -> CGSize {
        let imageSize: CGSize = viewModel.output.imageSizes.value[indexPath.row]
        let width: CGFloat = APP_WIDTH()
        let height: CGFloat = (imageSize.height * width) / max(1.0, imageSize.width)
        return CGSize(width: width, height: height)
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        insetForSectionAt section: Int
    ) -> UIEdgeInsets {
        let sideSpace: CGFloat = 20
        return UIEdgeInsets(top: sideSpace, left: 0, bottom: 0, right: 0)
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        minimumLineSpacingForSectionAt section: Int
    ) -> CGFloat {
        return 20
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        minimumInteritemSpacingForSectionAt section: Int
    ) -> CGFloat {
        return 20
    }

    public func collectionView(
        _ collectionView: UICollectionView,
        layout collectionViewLayout: UICollectionViewLayout,
        referenceSizeForHeaderInSection section: Int
    ) -> CGSize {
        let model: FetchNoticeEntity = viewModel.output.dataSource.value[section].model
        return CGSize(width: APP_WIDTH(), height: NoticeDetailHeaderView.getCellHeight(model: model))
    }
}
