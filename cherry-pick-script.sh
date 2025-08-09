#!/bin/bash

# Commits to cherry-pick (starting with most recent to avoid conflicts)
commits=(
    "30ee250"  # spotless plugin configuration
    "f705c0b"  # updated README.md
    "1a46a4d"  # added taskfiles, should help with running micro benchmarks
    "705e934"  # upgrading to JDK 24
    "9ce1bbc"  # non collator stop words
    "bca91d2"  # non collator stop words
    "12de5f4"  # fixed java version
    "244f1fe"  # bumped github actions
    "431876c"  # bumped versions
    "3ca35b8"  # added sequential map reduce unit test
    "a379bd8"  # work stealing pool
    "5dd4075"  # upgraded to JDK 21
    "0f1c272"  # auto cherry picking doesn't work
    "cd1a40c"  # playing with automatic cherry pick
    "ce3c3dd"  # playing with automatic cherry pick
    "668adc8"  # playing with automatic cherry pick
    "45fe670"  # updated java
    "68cd13b"  # applied spotless code formating
    "74131e4"  # upgraded JMH and added README
    "839a2fe"  # fixed missing maven wrapper
    "7f7b378"  # updated dependencies (likely to conflict)
)

# Target branches
branches=(
    "batching"
    "forkjoin"
    "observability"
    "parallel"
    "server"
    "virtualthreads"
    "workstealing"
)

# Store current branch
original_branch=$(git rev-parse --abbrev-ref HEAD)

echo "Starting cherry-pick process..."
echo "Will cherry-pick ${#commits[@]} commits to ${#branches[@]} branches"
echo

for branch in "${branches[@]}"; do
    echo "=== Processing branch: $branch ==="
    
    # Checkout the target branch
    git checkout "$branch"
    if [ $? -ne 0 ]; then
        echo "Failed to checkout $branch, skipping..."
        continue
    fi
    
    # Cherry-pick each commit
    success=true
    for commit in "${commits[@]}"; do
        echo "Cherry-picking $commit..."
        git cherry-pick "$commit"
        if [ $? -ne 0 ]; then
            echo "Cherry-pick failed for $commit on $branch"
            git cherry-pick --abort
            success=false
            break
        fi
    done
    
    if [ "$success" = true ]; then
        echo "✓ Successfully cherry-picked all commits to $branch"
    else
        echo "✗ Failed to cherry-pick some commits to $branch"
    fi
    
    echo
done

# Return to original branch
git checkout "$original_branch"
echo "Cherry-pick process completed!"
echo "Returned to original branch: $original_branch"
