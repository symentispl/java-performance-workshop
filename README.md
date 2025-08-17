# Java Performance Workshop

This project demonstrates how to work with performant code in Java using JMH microbenchmarks and profilers. It's based on a map-reduce framework implementation with a word count example, focusing on optimizing code performance through various approaches including sequential, parallel, batching, fork-join, and virtual thread implementations.

# Setup

## Prerequisites

To work with this project, you need the following tools installed:

### Required Tools

1. **Java 24** - The project uses Java 24 features (configured in pom.xml)
   - Download from [OpenJDK](https://jdk.java.net/24/) or use a distribution like [Amazon Corretto](https://aws.amazon.com/corretto/) or [Adoptium Temurin](https://adoptium.net/temurin/releases)
   - Verify installation: `java --version`

2. **Task** - Task runner used for build automation and benchmark execution
   - Install from [taskfile.dev](https://taskfile.dev/installation/)
   - On Linux/macOS: `sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d`
   - On Windows: `winget install Task.Task`
   - Verify installation: `task --version`

### Optional Tools

- **Maven** - Not required as the project includes Maven Wrapper (`./mvnw`)
- **async-profiler** - Automatically downloaded by the Task when running `task profile-benchmarks`

## Quick Start

1. Clone the repository
2. Run `task build` to build the project
3. Run `task run-benchmarks` to execute benchmarks
4. Run `task profile-benchmarks` to run with profiling (includes flamegraph generation)
