#!/bin/bash
# Setup git pre-commit hooks for the VEX Gateway project.
#
# This script installs a pre-commit hook that runs:
# - cargo fmt --check (formatting)
# - cargo clippy (linting)
#
# Run this script once after cloning the repository.

set -e

# Determine the hooks directory (works for both regular repos and worktrees)
# In a worktree, .git is a file; in a regular repo, .git is a directory
if [ -f ".git" ]; then
    # This is a worktree - .git is a file containing the path to the worktree git dir
    # Hooks must go in the main repo's hooks directory
    MAIN_GIT_DIR=$(git rev-parse --git-common-dir)
    HOOKS_DIR="$MAIN_GIT_DIR/hooks"
else
    HOOKS_DIR=".git/hooks"
fi

# Create the hooks directory if it doesn't exist
mkdir -p "$HOOKS_DIR"

PRE_COMMIT_HOOK="$HOOKS_DIR/pre-commit"

echo "Installing pre-commit hook to: $PRE_COMMIT_HOOK"

# Create the pre-commit hook
cat > "$PRE_COMMIT_HOOK" << 'EOF'
#!/bin/bash
# VEX Gateway pre-commit hook
# Runs formatting and linting checks before allowing commits.

set -e

echo "Running pre-commit checks..."

# Check for staged Rust files
STAGED_RS_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.rs$' || true)

if [ -z "$STAGED_RS_FILES" ]; then
    echo "No Rust files staged, skipping checks."
    exit 0
fi

# Run cargo fmt check
echo "Checking formatting with cargo fmt..."
if ! cargo fmt --check; then
    echo ""
    echo "ERROR: Formatting check failed!"
    echo "Run 'cargo fmt' to fix formatting issues."
    echo ""
    exit 1
fi
echo "Formatting OK."

# Run cargo clippy
echo "Running clippy lints..."
if ! cargo clippy --quiet -- -D warnings; then
    echo ""
    echo "ERROR: Clippy found issues!"
    echo "Fix the warnings above before committing."
    echo ""
    exit 1
fi
echo "Clippy OK."

echo "All pre-commit checks passed!"
EOF

# Make the hook executable
chmod +x "$PRE_COMMIT_HOOK"

echo "Pre-commit hook installed successfully!"
echo ""
echo "The hook will run 'cargo fmt --check' and 'cargo clippy' before each commit."
echo "To bypass the hook (not recommended), use: git commit --no-verify"
