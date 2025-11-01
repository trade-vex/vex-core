#!/bin/bash
set -euo pipefail

# ---- Setup ----
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"  # xtask/tests
WORKSPACE_ROOT="$(realpath "$SCRIPT_DIR/../..")"            # root of workspace
CACHE_DIR="$SCRIPT_DIR/.docker-cache-layer"
SRC_DIR="$CACHE_DIR/src"
TMP_DEPS="$CACHE_DIR/deps_raw.txt"
FINAL_DEPS="$CACHE_DIR/deps_final.txt"

echo "[*] Scanning workspace from root: $WORKSPACE_ROOT"
echo "[*] Creating dummy crate at: $CACHE_DIR"

rm -rf "$CACHE_DIR"
mkdir -p "$SRC_DIR"
touch "$TMP_DEPS" "$FINAL_DEPS"

# ---- Step 1: Extract dependencies from all Cargo.toml files ----
find "$WORKSPACE_ROOT" -name "Cargo.toml" -not -path "*/target/*" -not -path "$CACHE_DIR/*" | while read -r FILE; do
    echo "    └─ Parsing $FILE"
    awk '
    BEGIN { section = "" }
    /^\[workspace\.dependencies\]/ { section = "dependencies"; next }
    /^\[dependencies\]/           { section = "dependencies"; next }
    /^\[dev-dependencies\]/       { section = "dependencies"; next }
    /^\[build-dependencies\]/     { section = "dependencies"; next }
    /^\[/                         { section = ""; next }

    section == "dependencies" && NF {
        if ($0 ~ /=/ && $0 !~ /path\s*=/ && $0 !~ /workspace\s*=/)
            print $0
    }
    ' "$FILE" >> "$TMP_DEPS"
done

# ---- Step 2: Deduplicate by crate name ----
awk -F '=' '
{
    crate = $1;
    gsub(/ /, "", crate);
    if (!(crate in seen)) {
        seen[crate] = 1;
        print $0;
    }
}
' "$TMP_DEPS" | sort > "$FINAL_DEPS"

# ---- Step 3: Write Cargo.toml ----
cat > "$CACHE_DIR/Cargo.toml" <<EOF
[package]
name = "cache-deps"
version = "0.1.0"
edition = "2021"

[dependencies]
EOF

cat "$FINAL_DEPS" >> "$CACHE_DIR/Cargo.toml"

# ---- Step 4: Create dummy src/main.rs ----
echo 'fn main() {}' > "$SRC_DIR/main.rs"

# ---- Step 5: Clean up ----
rm "$TMP_DEPS" "$FINAL_DEPS"

echo "[✓] Dummy project ready: $CACHE_DIR"
