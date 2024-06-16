import SnapKit
import Then
import UIKit

public final class WarningV2View: UIView {

    private let imageView: UIImageView = UIImageView().then {
        $0.contentMode = .scaleAspectFill
        $0.image = DesignSystemAsset.Search.warning.image
    }

    private let label: WMLabel = WMLabel(
        text:"",
        textColor: DesignSystemAsset.BlueGrayColor.blueGray900.color,
        font: .t6(weight: .medium),
        kernValue: -0.5
    )
    


    public init(
        frame: CGRect,
        text: String
    ) {
        super.init(frame: frame)

        addSubviews()

        label.text = text

        setLayout()

    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }
}

extension WarningV2View {
    
    private func addSubviews() {
        self.addSubview(imageView)
        self.addSubview(label)
    }
    
    private func setLayout() {
        imageView.snp.makeConstraints {
            $0.width.height.equalTo(80)
            $0.top.equalToSuperview()
            $0.centerX.equalToSuperview()
        }

        label.snp.makeConstraints {
            $0.top.equalTo(imageView.snp.bottom).offset(-8)
            $0.leading.trailing.equalToSuperview()
            $0.centerX.equalTo(imageView.snp.centerX)
            $0.bottom.equalToSuperview()
        }

    }
}

