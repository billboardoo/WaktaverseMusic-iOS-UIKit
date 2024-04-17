import NVActivityIndicatorView
import ReactorKit
import RxSwift
import SnapKit
import Then
import UIKit

open class BaseReactorViewController<R: Reactor>: UIViewController, View {
    public var disposeBag = DisposeBag()
    open lazy var indicator = NVActivityIndicatorView(frame: .zero).then {
        view.addSubview($0)
        $0.snp.makeConstraints {
            $0.center.equalToSuperview()
            $0.size.equalTo(30)
        }
    }

    public init(reactor: R) {
        super.init(nibName: nil, bundle: nil)
        self.reactor = reactor
    }

    @available(*, unavailable)
    public required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    override open func viewDidLoad() {
        super.viewDidLoad()
        addView()
        setLayout()
        configureUI()
        configureNavigation()
    }

    public func bind(reactor: R) {
        bindState(reactor: reactor)
        bindAction(reactor: reactor)
    }

    open func addView() {}
    open func setLayout() {}
    open func configureUI() {
        // 내용 유지?
        if #available(iOS 15.0, *) {
            let tableViews = self.view.subviews
                .compactMap { $0 as? UITableView }
                .forEach {
                    $0.sectionHeaderTopPadding = 0
                }
        }
    }

    open func configureNavigation() {
        // 내용 유지?
        self.navigationController?.setNavigationBarHidden(true, animated: false)
    }

    open func bindState(reactor: R) {}
    open func bindAction(reactor: R) {}
}
