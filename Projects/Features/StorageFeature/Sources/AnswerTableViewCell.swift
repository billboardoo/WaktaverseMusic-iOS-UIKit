//
//  AnswerTableViewCell.swift
//  StorageFeature
//
//  Created by yongbeomkwak on 2023/01/30.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import UIKit
import DesignSystem

class AnswerTableViewCell: UITableViewCell {


    @IBOutlet weak var answerLabel: UILabel!
    
    override func awakeFromNib() {
        super.awakeFromNib()
        // Initialization code
        
        answerLabel.font = DesignSystemFontFamily.Pretendard.medium.font(size: 14)
        answerLabel.setLineSpacing(lineSpacing: 6)
    }

    

}

extension AnswerTableViewCell{
    public func update(model:QnAModel){
            
        
        answerLabel.text = model.ansewr
      

    }
    
    
}
