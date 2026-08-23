// Copyright 2024 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::process::Command as ProcessCommand;

use clap::Parser;
use clap::Subcommand;

#[derive(Parser)]
#[command(
    name = "cargo x",
    bin_name = "cargo x",
    about = "Run scopedb-client development tasks."
)]
struct Command {
    #[command(subcommand)]
    task: Task,
}

impl Command {
    fn run(self) {
        match self.task {
            Task::Check => run_command(make_check_command()),
            Task::Lint(args) => args.run(),
            Task::Test(args) => args.run(),
        }
    }
}

#[derive(Subcommand)]
enum Task {
    #[command(about = "Compile all targets with warnings denied.")]
    Check,
    #[command(about = "Run repository quality checks.")]
    Lint(LintArgs),
    #[command(about = "Run all tests.")]
    Test(TestArgs),
}

#[derive(Parser)]
struct LintArgs {
    #[arg(long, help = "Automatically apply lint and formatting fixes.")]
    fix: bool,
}

impl LintArgs {
    fn run(self) {
        run_command(make_clippy_command(self.fix));
        run_command(make_format_command(self.fix));
        ensure_installed("hawkeye", "hawkeye");
        run_command(make_hawkeye_command(self.fix));
        run_command(make_doc_command());
    }
}

#[derive(Parser)]
struct TestArgs {
    #[arg(long, help = "Run tests serially without capturing their output.")]
    no_capture: bool,
}

impl TestArgs {
    fn run(self) {
        run_command(make_test_command(self.no_capture));
    }
}

fn find_command(program: &str) -> ProcessCommand {
    let executable =
        which::which(program).unwrap_or_else(|err| panic!("{program} not found: {err}"));
    let mut command = ProcessCommand::new(executable);
    command.current_dir(env!("CARGO_WORKSPACE_DIR"));
    command
}

fn ensure_installed(binary: &str, package: &str) {
    if which::which(binary).is_err() {
        let mut command = find_command("cargo");
        command.args(["install", "--locked", package]);
        run_command(command);
    }
}

fn run_command(mut command: ProcessCommand) {
    println!("{command:?}");
    let status = command.status().expect("failed to execute command");
    assert!(status.success(), "command failed: {status}");
}

fn make_check_command() -> ProcessCommand {
    let mut command = find_command("cargo");
    command.env("RUSTFLAGS", "-D warnings");
    command.args([
        "+nightly",
        "check",
        "--workspace",
        "--locked",
        "--all-targets",
        "--all-features",
    ]);
    command
}

fn make_clippy_command(fix: bool) -> ProcessCommand {
    let mut command = find_command("cargo");
    command.args([
        "+nightly",
        "clippy",
        "--workspace",
        "--locked",
        "--tests",
        "--all-targets",
        "--all-features",
    ]);
    if fix {
        command.args(["--fix", "--allow-staged", "--allow-dirty"]);
    } else {
        command.args(["--", "-D", "warnings"]);
    }
    command
}

fn make_format_command(fix: bool) -> ProcessCommand {
    let mut command = find_command("cargo");
    command.args(["+nightly", "fmt", "--all"]);
    if !fix {
        command.arg("--check");
    }
    command
}

fn make_hawkeye_command(fix: bool) -> ProcessCommand {
    let mut command = find_command("hawkeye");
    command.arg(if fix { "format" } else { "check" });
    command
}

fn make_doc_command() -> ProcessCommand {
    let mut command = find_command("cargo");
    command.env("RUSTDOCFLAGS", "-D warnings");
    command.args([
        "+nightly",
        "doc",
        "--workspace",
        "--locked",
        "--all-features",
        "--no-deps",
    ]);
    command
}

fn make_test_command(no_capture: bool) -> ProcessCommand {
    let mut command = find_command("cargo");
    command.args([
        "test",
        "--workspace",
        "--locked",
        "--all-targets",
        "--all-features",
    ]);
    if no_capture {
        command.args(["--", "--nocapture", "--test-threads=1"]);
    }
    command
}

fn main() {
    Command::parse().run();
}
