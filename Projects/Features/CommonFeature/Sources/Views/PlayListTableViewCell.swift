//
//  PlayListTableViewCell.swift
//  StorageFeature
//
//  Created by yongbeomkwak on 2023/01/19.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import UIKit
import DesignSystem
import DomainModule
import Utility

class PlayListTableViewCell: UITableViewCell {
    @IBOutlet weak var button:UIButton!
    
    @IBOutlet weak var titleLabel: UILabel!
    
    @IBOutlet weak var albumImageView: UIImageView!
    @IBOutlet weak var artistLabel: UILabel!
    
    @IBAction func playOrEditAction(_ sender: UIButton) {
        
        if isEdit == false
        {
            print("Play")
        }
        
    }
    var isEdit:Bool = false
    
    
    override func awakeFromNib() {
        super.awakeFromNib()
        
        self.backgroundColor = .clear
        
        albumImageView.layer.cornerRadius = 4

        
        self.titleLabel.font = DesignSystemFontFamily.Pretendard.medium.font(size: 14)
        self.artistLabel.font = DesignSystemFontFamily.Pretendard.light.font(size: 12)

        
        self.titleLabel.textColor = DesignSystemAsset.GrayColor.gray900.color
        self.artistLabel.textColor = DesignSystemAsset.GrayColor.gray900.color

        

       
    }
    
    

   

}

extension PlayListTableViewCell {
    func update(_ model: SongEntity,_ isEditing:Bool) {

       
        albumImageView.kf.setImage(with: WMImageAPI.fetchYoutubeThumbnail(id: model.id).toURL,placeholder: DesignSystemAsset.Logo.placeHolderSmall.image,options: [.transition(.fade(0.2))])
        titleLabel.text =  model.title
        artistLabel.text = model.artist
        isEdit = isEditing
        
        button.setImage( isEditing ? DesignSystemAsset.Storage.move.image :  DesignSystemAsset.Storage.play.image, for: .normal)
        

        
    }
}
