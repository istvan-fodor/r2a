#!/bin/zsh

cargo fmt
if [[ "$(uname)" == "Darwin" ]]; then
    export CARGO_PROFILE_DEV_BUILD_OVERRIDE_DEBUG=true
    . ~/.zshrc
    mamba activate ros_env
    export DYLD_FALLBACK_LIBRARY_PATH=$AMENT_PREFIX_PATH/lib:$DYLD_FALLBACK_LIBRARY_PATH
elif [[ "$(uname)" == "Linux" ]]; then
    if [ -e "/opt/ros/jazzy/setup.bash" ]; then
        source "/opt/ros/jazzy/setup.bash"
    elif [ -e "/opt/ros/humble/setup.bash" ]; then
        source "/opt/ros/humble/setup.bash"
    fi
fi

cargo check --quiet --workspace --message-format=json --all-targets