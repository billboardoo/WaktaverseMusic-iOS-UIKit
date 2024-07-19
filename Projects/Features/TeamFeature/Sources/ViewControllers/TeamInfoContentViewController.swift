import DesignSystem
import Foundation
import LogManager
import RxCocoa
import RxSwift
import SnapKit
import Then
import UIKit
import Utility

final class TeamInfoContentViewController: UIViewController {
    private let tableView = UITableView().then {
        $0.register(TeamInfoListCell.self, forCellReuseIdentifier: "\(TeamInfoListCell.self)")
        $0.separatorStyle = .none
        $0.backgroundColor = .clear
        $0.sectionHeaderTopPadding = 0
        $0.allowsSelection = false
    }

    private let viewModel: TeamInfoContentViewModel
    lazy var input = TeamInfoContentViewModel.Input()
    lazy var output = viewModel.transform(from: input)
    private let disposeBag = DisposeBag()

    deinit {
        LogManager.printDebug("❌:: \(Self.self) deinit")
    }

    public init(viewModel: TeamInfoContentViewModel) {
        self.viewModel = viewModel
        super.init(nibName: nil, bundle: nil)
    }

    @available(*, unavailable)
    public required init?(coder: NSCoder) {
        fatalError("\(Self.self) has not been implemented")
    }

    override func viewDidLoad() {
        super.viewDidLoad()
        addSubviews()
        setLayout()
        configureUI()
        outputBind()
        inputBind()
    }
}

private extension TeamInfoContentViewController {
    func outputBind() {
        output.dataSource
            .skip(1)
            .debug()
            .bind(with: self, onNext: { owner, _ in
                owner.tableView.tableHeaderView = owner.output.type.value == .weeklyWM ?
                TeamInfoHeaderView(frame: .init(x: 0, y: 0, width: APP_WIDTH(), height: 140)) : nil
                owner.tableView.tableFooterView =
                TeamInfoFooterView(frame: .init(x: 0, y: 0, width: APP_WIDTH(), height: 88))
                owner.tableView.reloadData()
            })
            .disposed(by: disposeBag)
    }

    func inputBind() {
        input.combineTeamList.onNext(())
    }

    func addSubviews() {
        view.addSubviews(tableView)
    }

    func setLayout() {
        tableView.snp.makeConstraints {
            $0.top.equalTo(view.safeAreaLayoutGuide).offset(output.type.value == .develop ? 104 : 100)
            $0.horizontalEdges.bottom.equalToSuperview()
        }
    }

    func configureUI() {
        tableView.delegate = self
        tableView.dataSource = self
    }
}

extension TeamInfoContentViewController: TeamInfoSectionViewDelegate {
    func sectionTapped(with section: Int) {
        var newDataSource = output.dataSource.value
        newDataSource[section].model.isOpen = !newDataSource[section].model.isOpen
        output.dataSource.accept(newDataSource)

        tableView.reloadSections([section], with: .none)
        guard newDataSource[section].model.isOpen else { return }
        tableView.scrollToRow(at: IndexPath(row: 0, section: section), at: .middle, animated: true)
    }
}

extension TeamInfoContentViewController: UITableViewDelegate, UITableViewDataSource {
    func numberOfSections(in tableView: UITableView) -> Int {
        return output.dataSource.value.count
    }

    func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        return output.dataSource.value[section].model.isOpen ?
            output.dataSource.value[section].model.members.count : 0
    }

    func tableView(_ tableView: UITableView, heightForRowAt indexPath: IndexPath) -> CGFloat {
        return 4 + 40 + 4
    }

    func tableView(_ tableView: UITableView, heightForHeaderInSection section: Int) -> CGFloat {
        return 58
    }

    func tableView(_ tableView: UITableView, viewForHeaderInSection section: Int) -> UIView? {
        let sectionView = TeamInfoSectionView()
        sectionView.delegate = self
        sectionView.update(
            section: section,
            title: output.dataSource.value[section].title,
            isOpen: output.dataSource.value[section].model.isOpen
        )
        return sectionView
    }

    func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let cell = tableView.dequeueReusableCell(
            withIdentifier: "\(TeamInfoListCell.self)",
            for: indexPath
        ) as? TeamInfoListCell else {
            return UITableViewCell()
        }
        cell.update(entity: output.dataSource.value[indexPath.section].model.members[indexPath.row])
        return cell
    }

    func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {}
}
