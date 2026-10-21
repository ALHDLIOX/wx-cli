import AppKit
import Foundation

// App identity supplies a dedicated macOS privacy boundary for the sync worker.
// No permission prompts, automatic relaunches, or privacy database edits here.
let app = NSApplication.shared
app.setActivationPolicy(.accessory)
let root = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".wx-cli/nas-archive")
let config = try JSONSerialization.jsonObject(with: Data(contentsOf: root.appendingPathComponent("supervisor-config.json"))) as! [String: String]
let process = Process()
process.executableURL = URL(fileURLWithPath: config["python"]!)
process.arguments = [config["supervisor"]!]
var env = ProcessInfo.processInfo.environment
env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
process.environment = env
let log = root.appendingPathComponent("supervisor.log")
if !FileManager.default.fileExists(atPath: log.path) { FileManager.default.createFile(atPath: log.path, contents: nil, attributes: [.posixPermissions: 0o600]) }
let handle = try FileHandle(forWritingTo: log)
try handle.seekToEnd()
process.standardOutput = handle
process.standardError = handle
process.terminationHandler = { _ in DispatchQueue.main.async { app.terminate(nil) } }
try process.run()
app.run()
if process.isRunning { process.terminate() }
