import UIKit
import SnapKit
import Then
import DesignSystem

protocol BeforeSearchSectionHeaderViewDelegate : AnyObject {
    func tap() -> Void
}

class BeforeSearchSectionHeaderView:
    UICollectionReusableView {
    
    static let kind = "before-search-section-header"
    
    
    weak var delegate: BeforeSearchSectionHeaderViewDelegate?
    
    
    var label: UILabel = UILabel().then{
        $0.font = DesignSystemFontFamily.Pretendard.medium.font(size: 16)
        
        $0.textColor = DesignSystemAsset.BlueGrayColor.blueGray900.color
    }
    
    var button: UIButton = UIButton().then {
        $0.setTitle("전체보기", for: .normal)
        
        $0.setTitleColor(DesignSystemAsset.BlueGrayColor.blueGray900.color, for: .normal)
        
        $0.titleLabel?.font = DesignSystemFontFamily.Pretendard.medium.font(size: 14)
    }
    
    
    
    override init(frame: CGRect) {
        super.init(frame: frame)
        
        self.addSubviews(label,button)
        configureUI()
        button.addTarget(self, action: #selector(tap), for: .touchUpInside)
        
        self.backgroundColor = .orange
        
    }
    
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
    
}

extension BeforeSearchSectionHeaderView {
    
    func configureUI() {
        
        label.snp.makeConstraints {
            $0.leading.top.bottom.equalToSuperview()
        }
        
        button.snp.makeConstraints {
            $0.trailing.top.bottom.equalToSuperview()
        }
        
    }
    
    @objc func tap() {
        delegate?.tap()
    }
    
    public func update(_ title: String) {
        label.text = title
    }
    
}
