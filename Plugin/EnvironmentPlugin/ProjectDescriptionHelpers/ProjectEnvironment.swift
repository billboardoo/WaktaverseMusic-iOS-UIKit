import Foundation
import ProjectDescription

public struct ProjectEnvironment {
    public let previousName : String
    public let name: String
    public let organizationName: String
    public let deploymentTargets: DeploymentTargets
    public let destinations : Destinations
    public let baseSetting: SettingsDictionary

   
}

public let env = ProjectEnvironment(
    previousName: "Billboardoo",
    name: "WaktaverseMusic",
    organizationName: "yongbeomkwak",
    deploymentTargets: .iOS("14.0"),
    destinations: [.iPhone],
    baseSetting: SettingsDictionary()
        .marketingVersion("2.2.2")
        .currentProjectVersion("0")
        .debugInformationFormat(DebugInformationFormat.dwarfWithDsym)
        .otherLinkerFlags(["-ObjC"])
        .bitcodeEnabled(false)
)

