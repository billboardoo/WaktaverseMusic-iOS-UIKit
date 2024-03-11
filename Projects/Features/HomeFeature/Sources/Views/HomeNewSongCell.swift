//
//  HomeNewSongCell.swift
//  HomeFeature
//
//  Created by KTH on 2023/03/19.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import DesignSystem
import DomainModule
import Kingfisher
import SongsDomainInterface
import UIKit
import Utility

class HomeNewSongCell: UICollectionViewCell {
    @IBOutlet weak var albumImageView: UIImageView!
    @IBOutlet weak var titleStringLabel: UILabel!
    @IBOutlet weak var artistLabel: UILabel!
    @IBOutlet weak var playImageView: UIImageView!

    override func awakeFromNib() {
        super.awakeFromNib()

        self.backgroundColor = .clear
        self.contentView.backgroundColor = .clear

        albumImageView.layer.cornerRadius = 8
        albumImageView.clipsToBounds = true
        albumImageView.contentMode = .scaleAspectFill
        playImageView.image = DesignSystemAsset.Home.playSmall.image
    }
}

extension HomeNewSongCell {
    func update(model: NewSongsEntity) {
        let titleAttributedString = NSMutableAttributedString(
            string: model.title,
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.medium.font(size: 14),
                .foregroundColor: DesignSystemAsset.GrayColor.gray900.color,
                .kern: -0.5
            ]
        )
        titleStringLabel.attributedText = titleAttributedString

        let artistAttributedString = NSMutableAttributedString(
            string: model.artist,
            attributes: [
                .font: DesignSystemFontFamily.Pretendard.light.font(size: 12),
                .foregroundColor: DesignSystemAsset.GrayColor.gray900.color,
                .kern: -0.5
            ]
        )
        artistLabel.attributedText = artistAttributedString

        albumImageView.kf.setImage(
            with: URL(string: WMImageAPI.fetchYoutubeThumbnail(id: model.id).toString),
            placeholder: DesignSystemAsset.Logo.placeHolderMedium.image,
            options: [.transition(.fade(0.2))]
        )
    }
}
