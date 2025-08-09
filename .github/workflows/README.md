# Auto Merge Workflow

This workflow automatically propagates commits through a defined branch chain using git merge and pull requests.

## Branch Chain

The current propagation chain is:
```
master → parallel → batching → workstealing → virtualthreads → server
```

## How It Works

1. **Trigger**: When you push commits to any branch in the chain (except `server`)
2. **Check**: The workflow checks if the target branch is behind the source branch
3. **Merge**: Creates a new branch and attempts to merge the source branch
4. **PR Creation**: Creates a pull request from the merge branch to the target branch

## Workflow Behavior

### ✅ No Conflicts
- Merge completes successfully
- PR is labeled `auto-merge` and `ready-to-merge`
- PR is ready for review and merge

### ❌ Conflicts Detected
- **Workflow fails immediately** when conflicts are detected
- No PR is created
- Manual merge resolution is required

## Manual Conflict Resolution

When the workflow fails due to merge conflicts, resolve them manually:

1. **Checkout the target branch locally:**
   ```bash
   git checkout <target-branch>
   git pull origin <target-branch>
   ```

2. **Attempt the merge:**
   ```bash
   git merge <source-branch>
   ```

3. **Resolve conflicts:**
   ```bash
   # Edit conflicted files to resolve conflicts
   # Remove conflict markers and choose the correct code
   
   # Add resolved files
   git add <resolved-files>
   
   # Complete the merge
   git commit
   ```

4. **Push the resolved merge:**
   ```bash
   git push origin <target-branch>
   ```

## Manual Triggering

You can manually trigger the workflow for any source/target combination:

1. Go to Actions → Auto Merge PR
2. Click "Run workflow"
3. Select source and target branches
4. Click "Run workflow"

## Configuration

The branch chain is configured in the workflow file under `env.BRANCH_CHAIN`. To modify:

1. Edit `.github/workflows/auto-merge-pr.yml`
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

- **Success**: `auto-merge`, `ready-to-merge`
- **Conflicts**: Workflow fails, no PR created

## Branch Naming

Merge branches follow this pattern:
```
merge/{source}-to-{target}-{timestamp}
```

Example: `merge/master-to-parallel-20250809-102855`

## Cleanup

After merging a merge PR, you can safely delete the merge branch as it's no longer needed.
