# Auto Cherry-Pick Workflow

This workflow automatically propagates commits through a defined branch chain using cherry-picks and pull requests.

## Branch Chain

The current propagation chain is:
```
master → parallel → batching → workstealing → virtualthreads → server
```

## How It Works

1. **Trigger**: When you push commits to any branch in the chain (except `server`)
2. **Detection**: The workflow finds commits that exist in the source branch but not in the target branch
3. **Cherry-pick**: Creates a new branch and attempts to cherry-pick the commits
4. **PR Creation**: Creates a pull request from the cherry-pick branch to the target branch

## Workflow Behavior

### ✅ No Conflicts
- All commits cherry-pick cleanly
- PR is labeled `auto-cherry-pick` and `ready-to-merge`
- PR is ready for review and merge

### ⚠️ Conflicts Detected
- Commits with conflicts are committed with conflict markers
- PR is labeled `auto-cherry-pick`, `conflicts`, and `manual-resolution-needed`
- Manual resolution is required (see instructions in the PR)

## Manual Conflict Resolution

When conflicts occur, follow these steps:

1. **Checkout the cherry-pick branch:**
   ```bash
   git checkout cherry-pick/source-to-target-timestamp
   ```

2. **Find commits with conflicts:**
   ```bash
   git log --oneline --grep="CONFLICTS"
   ```

3. **For each conflicted commit:**
   ```bash
   # Reset to the commit before the conflicted one
   git reset --hard HEAD~1
   
   # Re-apply the cherry-pick interactively
   git cherry-pick <commit-hash>
   
   # Resolve conflicts manually
   # Edit the conflicted files
   
   # Add resolved files
   git add <resolved-files>
   
   # Continue cherry-pick
   git cherry-pick --continue
   ```

4. **Push the resolved changes:**
   ```bash
   git push origin cherry-pick/source-to-target-timestamp
   ```

## Manual Triggering

You can manually trigger the workflow for any source/target combination:

1. Go to Actions → Auto Cherry-Pick PR
2. Click "Run workflow"
3. Select source and target branches
4. Click "Run workflow"

## Configuration

The branch chain is configured in the workflow file under `env.BRANCH_CHAIN`. To modify:

1. Edit `.github/workflows/auto-cherry-pick-pr.yml`
2. Update the `BRANCH_CHAIN` environment variable
3. Follow the format: `source_branch=target_branch`

Example:
```yaml
env:
  BRANCH_CHAIN: |
    master=parallel
    parallel=batching
    batching=workstealing
    workstealing=virtualthreads
    virtualthreads=server
```

## Labels

The workflow automatically adds labels to PRs:

- **No conflicts**: `auto-cherry-pick`, `ready-to-merge`
- **With conflicts**: `auto-cherry-pick`, `conflicts`, `manual-resolution-needed`

## Branch Naming

Cherry-pick branches follow this pattern:
```
cherry-pick/{source}-to-{target}-{timestamp}
```

Example: `cherry-pick/master-to-parallel-20250809-102855`

## Cleanup

After merging a cherry-pick PR, you can safely delete the cherry-pick branch as it's no longer needed.
