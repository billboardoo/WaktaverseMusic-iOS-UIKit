import Foundation

public struct QnaEntity: Equatable {
    public init(
        create_at: Int,
        category: String,
        question: String,
        description: String,
        isOpen: Bool
    ) {
        self.create_at = create_at
        self.category = category
        self.question = question
        self.description = description
        self.isOpen = isOpen
    }
    public let create_at: Int
    public let category,question,description:String
    public var isOpen:Bool
}
