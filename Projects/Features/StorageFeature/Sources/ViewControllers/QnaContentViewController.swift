//
//  QnaContentViewController.swift
//  StorageFeature
//
//  Created by yongbeomkwak on 2023/01/29.
//  Copyright © 2023 yongbeomkwak. All rights reserved.
//

import UIKit
import Utility
import RxSwift
import RxCocoa
import RxRelay
import CommonFeature

public final class QnaContentViewController: UIViewController, ViewControllerFromStoryBoard {
    @IBOutlet weak var tableView: UITableView!
    
    var viewModel:QnaContentViewModel!
    
    public override func viewDidLoad() {
        super.viewDidLoad()
        configureUI()
    }
    
    public override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        navigationController?.interactivePopGestureRecognizer?.delegate = nil //스와이프로 뒤로가기
    }
    
    public static func viewController(viewModel: QnaContentViewModel) -> QnaContentViewController {
        let viewController = QnaContentViewController.viewController(storyBoardName: "Storage", bundle: Bundle.module)
                viewController.viewModel = viewModel
        return viewController
    }
}

extension QnaContentViewController{
    private func configureUI(){
        tableView.dataSource = self
        tableView.delegate = self
        let space = APP_HEIGHT() - 106 - STATUS_BAR_HEGHIT() - SAFEAREA_BOTTOM_HEIGHT()
        let height = space / 3 * 2
        let warningView = WarningView(frame: CGRect(x: 0, y: 0, width: APP_WIDTH(), height: height))
        warningView.text = "자주 묻는 질문이 없습니다."
        tableView.tableFooterView = viewModel.dataSource.isEmpty ? warningView : UIView(frame: CGRect(x: 0, y: 0, width: APP_WIDTH(), height: 56))
        tableView.scrollIndicatorInsets = UIEdgeInsets(top: 0, left: 0, bottom: 56, right: 0)
        tableView.reloadData()
    }
    
    private func scrollToBottom(indexPath:IndexPath){
        DispatchQueue.main.async {
            self.tableView.scrollToRow(at: indexPath, at: .middle, animated: true)
        }
    }
}

extension QnaContentViewController:UITableViewDataSource{
    public func numberOfSections(in tableView: UITableView) -> Int {
        return viewModel.dataSource.count
    }
    
    public func tableView(_ tableView: UITableView, numberOfRowsInSection section: Int) -> Int {
        var data = viewModel.dataSource[section]
        var count:Int = 0
        
        if data.isOpen {
            count = 2
        }else{
            count = 1
        }
        return count
    }
    
    public func tableView(_ tableView: UITableView, cellForRowAt indexPath: IndexPath) -> UITableViewCell {
        guard let Qcell = tableView.dequeueReusableCell(withIdentifier: "QuestionTableViewCell", for: indexPath) as? QuestionTableViewCell else{
            return UITableViewCell()
        }
        guard let Acell = tableView.dequeueReusableCell(withIdentifier: "AnswerTableViewCell", for: indexPath) as? AnswerTableViewCell else {
            return UITableViewCell()
        }
        
        var data = viewModel.dataSource
        
        Qcell.update(model: data[indexPath.section])
        Qcell.selectionStyle = .none // 선택 시 깜빡임 방지
        Acell.update(model: data[indexPath.section])
        Acell.selectionStyle = .none
        
        // 왜 Row는  인덱스 개념이다 0 부터 시작??
        if indexPath.row == 0 {
            return Qcell
        }else {
            return Acell
        }
    }
}

extension QnaContentViewController:UITableViewDelegate{
    
    public func tableView(_ tableView: UITableView, didSelectRowAt indexPath: IndexPath) {
        
        tableView.deselectRow(at: indexPath, animated: true)
        
        var data = viewModel.dataSource
        
        data[indexPath.section].isOpen = !data[indexPath.section].isOpen
        
        viewModel.dataSource = data
        
        tableView.reloadSections([indexPath.section], with: .none)
        
        
        let next = IndexPath(row: 1, section: indexPath.section  ) //row 1이 답변 쪽
        
        
        if data[indexPath.section].isOpen
        {
            self.scrollToBottom(indexPath: next)
        }
    }
    
    public func tableView(_ tableView: UITableView, heightForRowAt indexPath: IndexPath) -> CGFloat {
        return UITableView.automaticDimension
    }
}
