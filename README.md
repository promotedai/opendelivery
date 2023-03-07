# OpenDelivery (Preview)
[Promoted.ai](http://Promoted.ai): Open and Source Available search and ranking service in C++

Promoted is going open! We are in the **process of publishing the source code** that runs our internal hosted products to share with our customers and our community. In the spirit of showing progress and being open, we've published and licensed our progress to date, and we will continue to do so as we invest in editing and organizing our formerly closed source code as open for our community. Please contact us if you have any questions or would like to collaborate or use this source code.

We are currently porting our managed service from Golang to C++. If you are a current customer and would like source-available access of the Golang implementation, please email Andrew.

The following pieces of delivery are still in the progress of being ported and will be shared when they are ready:
- Predictors
- Blender

# Foreword

All example commands are run from this directory.

To checkout the repo:
```
git clone git@github.com:promotedai/delivery-cpp.git
git submodule update --init --recursive
```

The Docker instructions assume that you've [installed Docker](https://docs.docker.com/get-docker/). 

## Table of contents
[Code structure](#code-structure)  
[Installation](#installation)  
[Running](#running)  
[Tools](#tools)  

## Code structure

(This would work better as slides but this will have to do for now.)

The delivery service is much like any other service:
1. A request is received
2. Some processing is done
3. A respond is returned

The difficulty in doing this at scale in C++ is that:
- There is no built-in scheduler. In brief, to emulate this you generally make a thread pool and they take turns polling for requests.
    - This is what [Drogon](https://github.com/drogonframework/drogon) offers. It allows us to just think about #2. For more information about Drogon, [this documentation](https://github.com/drogonframework/drogon/wiki/ENG-FAQ-1-Understanding-drogon-threading-model) is pretty informative.
- There are no coroutines. Async behavior is non-negotiable for performance, and the only alternative is passing around callback functions.
    - C++20 actually has coroutines, and Drogon supports them, but most things don't. For example, the async calls in the [AWS SDK for C++](https://github.com/aws/aws-sdk-cpp) work via callbacks. And big tech does not move to new versions of C++ quickly.

The structure of the codebase is primarily meant to try and allow most of our business logic to avoid thinking about this stuff. Some of the more central pieces:
- `main`
    - This initializes most global state and starts listening for requests.
- `controllers`
    - These are the handlers for the different API endpoints. These are static instances shared by all threads. These can access - and sometimes modify - global state. `deliver.cc` corresponds to the `/deliver` endpoint and sets up an _executor_ which handles most processing for a given request.
- `singletons`
    - These are the non-Drogon-owned global states. These can only be linked into the above two targets. It was a goal to keep global state outside of most business logic. Having short parameter lists and being able to access anything from anywhere is convenient at first, but it quickly becomes unmaintainable in C++. And use of the language at all means you want to build something to last.
- `execution`
    - An executor is a per-request abstraction for what actually drives the processing. Since processing includes an arbitrary number of callbacks (to avoid waiting on synchronous calls), the controllers must return before an execution even begins. This necessarily depends on some of Drogon's global state: its event loops.
- `stages`
    - This is where most business logic belongs. To quote [stage.h](execution/stages/stage.h): "_A stage can be thought of as a discrete unit of work. Inputs and outputs are specified as parameters of derived class constructors, and should be owned by something other than the stage. A stage can assume that its inputs are ready to be used by the time run() is called._" In practice the inputs and outputs should be owned by a `Context`, which is 1:1 with the executor driving the stages.

## Installation

The definitive list of dependencies is _only_ maintained in [our CI container](docker/Dockerfile). Despite its convenience, it will slow down how quickly you can iterate. If you intend to do a lot of development in this repo, the other installation methods may be preferable.

### Docker instructions

For the entire service:
```
docker build -t delivery -f docker/Dockerfile .
```

For just tests:
```
docker build -t delivery --target tester -f docker/Dockerfile .
```

Notes:
- Building copies the repo into the image, so to see your code changes reflected in the image you must build again.
- The Dockerfile doesn't do any sanitization of your repo clone. If you've been running CMake manually and Docker complains about CMake, run `rm -rf build/` and try again.

### Ubuntu instructions

Copy commands up to the `builder` stage of [our CI container](docker/Dockerfile). May need to change a few things.

And note that you do not need to install (or `cp`) the binary. You can just run it while it sits in the `build/` directory.

### macOS instructions

Same as the Ubuntu instructions, but commands from our CI container will not work outright. Comparable dependencies will have to be found and installed via Homebrew or something similar.

Your compiler paths will probably be like this instead:
```
/Library/Developer/CommandLineTools/usr/bin/clang
/Library/Developer/CommandLineTools/usr/bin/clang++
```

And getting your number of processors will be like this:
```
$(sysctl -n hw.logicalcpu)
```

### Devcontainer instructions (EXPERIMENTAL)

This repo contains a `.devcontainer/devcontainer.json` file which allows to do development in Docker with the [Remote - Containers](https://code.visualstudio.com/docs/remote/containers) extension for VS Code. To open the environment, either click on the pop-up up that appears in VS Code when the project is opened or select "Rebuild and Reopen in Container" in the command palette. "Reopen in Container" command can be used to re-open the most recent container built.

To exit from from the devcontainer environment, select "Dev Containers: Reopen Folder Locally"

## Running

Note that, for the time being, running the service requires a bunch of environment variables for the specific platform that you're targetting.

### With Docker

For the entire service (second example additionally passes env vars):
```
docker run -p 127.0.0.1:9090:9090/tcp -t delivery
docker run -p 127.0.0.1:9090:9090/tcp -e API_KEY -e CONFIG_PATHS -t delivery
```

For just tests (see how arguments are used in [tester_entrypoint.sh](docker/tester_entrypoint.sh)):
```
docker run -t delivery "" off on
```

### Otherwise

For the entire service:
```
./build/main
```

For just tests (see the Docker version for parallelizing or official CTest documentation for targeting specific tests):
```
ctest --test-dir build --output-on-failure
```

## Tools

[Linting](https://clang.llvm.org/extra/clang-tidy/) and running tests under [sanitizers](https://github.com/google/sanitizers) can detect concrete, immediate issues and thus run as part of our CI. [Formatting](https://clang.llvm.org/docs/ClangFormat.html) and [IWYU](https://include-what-you-use.org/) are not automated and we discuss why inline.

### Linting

If you're using Docker, the linter can be run with:
```
docker run -t delivery "" on off
```

Otherwise, the linter can be run with:
```
python3 run-clang-tidy.py
```

If any checks are causing the linter to fail on your PR and it's not a legitimate issue, feel free to add more exceptions to [.clang-tidy](.clang-tidy).

### Sanitizers

C++ gives you a lot of ways to shoot yourself in the foot, and sanitizers are probably the best defense against it. Adding `-DENABLE_ASAN=ON` to a `cmake` command will enable AddressSanitizer and `-DENABLE_TSAN=ON` will enable ThreadSanitizer. These are disabled by default because they significantly increase build time, run times, and only one can be used at a time anyway.

### Formatting

Our [.clang-format](.clang-format) can be easily applied at any time. All IDEs will have shortcuts to apply it to the current file or sections of files.

This is not automated because there was not a good, pre-existing action for applying fixes - only for recognizing that there were issues. It also does not break anything and can be fixed later (at the cost of some blame history).

### IWYU

`#include`s in C++ are fundamentally transitive, but this is a maintenance risk. We could, for example, include Drogon headers everywhere and transitively get the standard headers for free because the Drogon headers include them. If the Drogon headers ever change, updating our Drogon submodule commit would require a widespread fix of includes as well.

To run, build the project and run something like (`main.cc` is an example; it has to be run individually on all modified `.cc` files):
```
/usr/bin/iwyu_tool -p build/ main.cc
```

This is not automated because it's an "alpha" tool. It would have to be wrapped in a script to be useful at scale and can plainly contradict itself in some cases. Note that there are also rare cases in which you want certain files to _only_ be included transitively.
