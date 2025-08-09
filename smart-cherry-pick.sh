#!/bin/bash

# Recent commits that are more likely to apply cleanly
priority_commits=(
    "30ee250"  # spotless plugin configuration
    "f705c0b"  # updated README.md
    "1a46a4d"  # added taskfiles (may have conflicts)
)

# Medium priority commits  
medium_commits=(
    "9ce1bbc"  # non collator stop words
    "bca91d2"  # non collator stop words
    "12de5f4"  # fixed java version
    "244f1fe"  # bumped github actions
    "431876c"  # bumped versions
    "3ca35b8"  # added sequential map reduce unit test
)

# Lower priority commits that may conflict
lower_commits=(
    "705e934"  # upgrading to JDK 24 (likely conflicts)
    "a379bd8"  # work stealing pool
    "5dd4075"  # upgraded to JDK 21
)

# Target branches (excluding batching since we already worked on it)
branches=(
    "forkjoin"
    "observability"
    "parallel"
    "server"
    "virtualthreads"
    "workstealing"
)

# Store current branch
original_branch=$(git rev-parse --abbrev-ref HEAD)

echo "=== Smart Cherry-Pick Process ==="
echo "Processing ${#branches[@]} remaining branches"
echo "Priority commits: ${#priority_commits[@]}"
echo "Medium commits: ${#medium_commits[@]}"  
echo "Lower priority commits: ${#lower_commits[@]}"
echo

process_commits() {
    local branch=$1
    local commits=("${@:2}")
    
    echo "Processing ${#commits[@]} commits for $branch..."
    
    for commit in "${commits[@]}"; do
        echo "  Cherry-picking $commit..."
        git cherry-pick "$commit" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            echo "  ✓ Success"
        else
            echo "  ✗ Conflict - skipping"
            git cherry-pick --abort 2>/dev/null
            return 1  # Stop processing this category for this branch
        fi
    done
    
    return 0
}

for branch in "${branches[@]}"; do
    echo "=== Processing branch: $branch ==="
    
    # Checkout the target branch
    git checkout "$branch" 2>/dev/null
    if [ $? -ne 0 ]; then
        echo "Failed to checkout $branch, skipping..."
        continue
    fi
    
    # Process priority commits first
    echo "Processing priority commits..."
    if process_commits "$branch" "${priority_commits[@]}"; then
        echo "All priority commits applied successfully"
        
        # Try medium priority commits
        echo "Processing medium priority commits..."
        if process_commits "$branch" "${medium_commits[@]}"; then
            echo "All medium priority commits applied successfully"
            
            # Try lower priority commits
            echo "Processing lower priority commits..."
            process_commits "$branch" "${lower_commits[@]}"
        fi
    fi
    
    # Show what we accomplished
    echo "Final state of $branch:"
    git --no-pager log --oneline "$branch" -3
    echo
done

# Return to original branch
git checkout "$original_branch"
echo "=== Process completed! ==="
echo "Returned to original branch: $original_branch"

# Show summary
echo
echo "=== Summary ==="
echo "Check each branch to see what commits were successfully applied:"
for branch in "batching" "${branches[@]}"; do
    echo "- $branch: $(git rev-list --count $branch) commits"
done
