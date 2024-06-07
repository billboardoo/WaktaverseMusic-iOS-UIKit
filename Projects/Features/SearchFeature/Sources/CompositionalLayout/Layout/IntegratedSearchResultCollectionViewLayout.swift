import UIKit
import Utility

final class IntegratedSearchResultCollectionViewLayout: UICollectionViewCompositionalLayout {
    init() {
        super.init { _, _ in

            return IntegratedSearchResultCollectionViewLayout.configureLayout()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}

extension IntegratedSearchResultCollectionViewLayout {
    private static func configureLayout() -> NSCollectionLayoutSection {
        let itemSize = NSCollectionLayoutSize(
            widthDimension: .fractionalWidth(1.0),
            heightDimension: .fractionalHeight(1.0)
        )

        let headerLayout = NSCollectionLayoutSize(widthDimension: .fractionalWidth(1.0), heightDimension: .absolute(30))

        let header = NSCollectionLayoutBoundarySupplementaryItem(
            layoutSize: headerLayout,
            elementKind: IntegratedSearchResultHeaderView.kind,
            alignment: .top
        )

        var item: NSCollectionLayoutItem = NSCollectionLayoutItem(layoutSize: itemSize)
        let groupSize = NSCollectionLayoutSize(
            widthDimension: .fractionalWidth(1.0),
            heightDimension: .fractionalWidth(0.16)
        )
        let group = NSCollectionLayoutGroup.horizontal(layoutSize: groupSize, subitems: [item])

        let section = NSCollectionLayoutSection(group: group)
        section.contentInsets = NSDirectionalEdgeInsets(top: .zero, leading: 20.0, bottom: 20.0, trailing: 20.0)
        section.boundarySupplementaryItems = [header]

        return section
    }
}
