//
//  NoticeDetailHeaderView.swift
//  StorageFeature
//
//  Created by KTH on 2023/04/08.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import UIKit
import DomainModule
import DesignSystem
import Utility

class NoticeDetailHeaderView: UICollectionReusableView {
        
    @IBOutlet weak var titleStringLabel: UILabel!
    @IBOutlet weak var dateLabel: UILabel!
    @IBOutlet weak var timeLabel: UILabel!
    @IBOutlet weak var contentStringLabel: UILabel!
        
    override func awakeFromNib() {
        super.awakeFromNib()
        
        titleStringLabel.font = DesignSystemFontFamily.Pretendard.medium.font(size: 18)
        titleStringLabel.textColor = DesignSystemAsset.GrayColor.gray900.color
        titleStringLabel.setLineSpacing(kernValue: -0.5, lineSpacing: 0, lineHeightMultiple: 1.26)

        dateLabel.font = DesignSystemFontFamily.Pretendard.light.font(size: 12)
        dateLabel.textColor = DesignSystemAsset.GrayColor.gray500.color
        dateLabel.setLineSpacing(kernValue: -0.5, lineSpacing: 0, lineHeightMultiple: 0)
        
        timeLabel.font = DesignSystemFontFamily.Pretendard.light.font(size: 12)
        timeLabel.textColor = DesignSystemAsset.GrayColor.gray500.color
        timeLabel.setLineSpacing(kernValue: -0.5, lineSpacing: 0, lineHeightMultiple: 0)
        
        contentStringLabel.font = DesignSystemFontFamily.Pretendard.medium.font(size: 14)
        contentStringLabel.textColor = DesignSystemAsset.GrayColor.gray900.color
        contentStringLabel.setLineSpacing(kernValue: -0.5, lineSpacing: 0, lineHeightMultiple: 1.26)
    }
}

extension NoticeDetailHeaderView {
    
    static func getCellHeight(model: FetchNoticeEntity) -> CGFloat {
        let availableWidth: CGFloat = APP_WIDTH()-40
        let paragraphStyle = NSMutableParagraphStyle()
        paragraphStyle.lineHeightMultiple = 1.26
        
        let titleAttributedString = NSMutableAttributedString(
            string: model.title,
            attributes: [.font: DesignSystemFontFamily.Pretendard.medium.font(size: 18),
                         .foregroundColor: DesignSystemAsset.GrayColor.gray900.color,
                         .kern: -0.5,
                         .paragraphStyle: paragraphStyle]
        )
        let titleHeight: CGFloat = max(28, titleAttributedString.height(containerWidth: availableWidth))

        let contentString: String = model.content ?? "팬치들 스샷 참고하세요! 🥰"
        let contentAttributedString = NSMutableAttributedString(
            string: contentString,
            attributes: [.font: DesignSystemFontFamily.Pretendard.medium.font(size: 14),
                         .foregroundColor: DesignSystemAsset.GrayColor.gray900.color,
                         .kern: -0.5,
                         .paragraphStyle: paragraphStyle]
        )
        let contentHeight: CGFloat = contentString.isEmpty ? 0 : contentAttributedString.height(containerWidth: availableWidth)

        let baseHeight: CGFloat = 12 + 3 + 18 + 20 + 1 + 20
        return titleHeight + contentHeight + baseHeight
    }
    
    func update(model: FetchNoticeEntity) {
        titleStringLabel.text = model.title
        dateLabel.text = (model.createAt/1000.0).unixTimeToDate.dateToString(format: "yy.MM.dd")
        timeLabel.text = (model.createAt/1000.0).unixTimeToDate.dateToString(format: "HH:mm")
        contentStringLabel.text = model.content ?? "팬치들 스샷 참고하세요! 🥰"
    }
}
