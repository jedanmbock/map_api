#!/bin/bash

# --- Configuration ---

# Default project path if none provided
DEFAULT_PROJECT_PATH="."
PROJECT_PATH=${1:-"$DEFAULT_PROJECT_PATH"}

# Output file name (relative to PROJECT_PATH)
OUTPUT_FILENAME="project_context.txt"

# Directories to completely ignore (won't be traversed)
EXCLUDE_DIRS_PATTERN=( \
    ".*"            # All hidden folders (.git, .vscode, .idea, .svn, etc.)
    "node_modules"
    "vendor"        # PHP Composer
    "build"
    "dist"
    "target"        # Java/Rust build outputs
    "__pycache__"   # Python cache
    ".next"         # Next.js build output
    "cache"         # General cache folders
    "target"
    "venv"
    "shapes"
    "storage"       # Laravel storage (often contains logs, cache, etc.)
    # Add more directory names here if needed
)

# Specific file patterns to ignore within traversed directories
EXCLUDE_FILES_PATTERN=( \
    "*.log"
    "*.jar"
    "*.pdf"
    "*.png"
    "*.jpg"
    "*.class"
    "*.sqlite"
    "*.csv"
    "project_context.txt"
    # ".env*"       # Consider if you NEED .env files; uncomment if NOT needed.
    "package-lock.json"
    "yarn.lock"
    "composer.lock"
    "*.ico"
    "pnpm-lock.yaml"
    # Add more file patterns here (e.g., "*.swp", "*.bak", "*.tmp")
)

# --- Script Logic ---

# Attempt to get absolute path; exit if PROJECT_PATH is invalid early
PROJECT_PATH=$(realpath "$PROJECT_PATH" 2>/dev/null)
if [ $? -ne 0 ] || [ ! -d "$PROJECT_PATH" ]; then
    echo "Error: Invalid or non-existent project directory specified." >&2 # Error to stderr
    exit 1
fi

OUTPUT_FILE="$PROJECT_PATH/$OUTPUT_FILENAME"

# --- Safety Check: Prevent overwriting the project directory itself ---
# This is unlikely but guards against strange configurations
if [ "$PROJECT_PATH" == "$OUTPUT_FILE" ]; then
    echo "Error: Project directory path conflicts with output file name '$OUTPUT_FILENAME'." >&2
    exit 1
fi

# Delete output file silently if it exists
rm -f "$OUTPUT_FILE"

# --- Build the find command ---
# Uses arrays to construct the find command safely and avoid complex escaping issues with eval
find_args=("$PROJECT_PATH")

# Add directory prune conditions
if [ ${#EXCLUDE_DIRS_PATTERN[@]} -gt 0 ]; then
    find_args+=(\()
    first_prune=true
    for dir_pattern in "${EXCLUDE_DIRS_PATTERN[@]}"; do
        if ! $first_prune; then
            find_args+=(-o)
        fi
        find_args+=(-name "$dir_pattern" -type d)
        first_prune=false
    done
    find_args+=(\) -prune -o) # Add the prune action and the OR for the next part
fi

# Add primary find conditions (type file, exclude output file, exclude patterns)
find_args+=(\( -type f -not -path "$OUTPUT_FILE")
if [ ${#EXCLUDE_FILES_PATTERN[@]} -gt 0 ]; then
    for file_pattern in "${EXCLUDE_FILES_PATTERN[@]}"; do
        find_args+=(-not -name "$file_pattern")
    done
fi
find_args+=(-print \)) # Add the print action and close the group

# --- Execute the find command and process results ---

# Create the header in the output file
{
    echo "Project Context From: $PROJECT_PATH"
    echo "Generated On: $(date)"
    echo "==============================================="
    echo "Ignored Directory Patterns: ${EXCLUDE_DIRS_PATTERN[*]}"
    echo "Ignored File Patterns: ${EXCLUDE_FILES_PATTERN[*]}"
    echo "==============================================="
    echo ""
} > "$OUTPUT_FILE"

error_count=0
# Use find with process substitution and sorting. Avoids eval.
while IFS= read -r FILE_PATH; do
    # Calculate relative path for cleaner output
    RELATIVE_PATH="${FILE_PATH#"$PROJECT_PATH"/}"

    # Append file info and content to the output file
    {
        # echo ""
        # echo "// ==============================================="
        # echo "---> FILE: $RELATIVE_PATH"
        echo "//---> PATH: $FILE_PATH"
        # echo "// ==============================================="
        echo ""
    } >> "$OUTPUT_FILE"

    # Check if file is likely binary/non-text using 'file' command
    # -b: omit filename; check for common non-text types
    if file -b "$FILE_PATH" | grep -q -E 'binary|archive|compressed|image|font'; then
        echo "[Non-text file (e.g., binary, data, compressed) - Contents omitted]" >> "$OUTPUT_FILE"
    else
        # Append text file content, redirect cat errors to stderr
        if ! cat "$FILE_PATH" >> "$OUTPUT_FILE" 2> /dev/null; then # Hide cat errors from stdout
             # Optionally log error to the output file itself, or just count it
             echo "[Error reading file content for $RELATIVE_PATH]" >> "$OUTPUT_FILE"
             ((error_count++))
        fi
    fi

    {
        # echo ""
        echo "// END OF FILE: $RELATIVE_PATH"
        echo ""
    } >> "$OUTPUT_FILE"

done < <(find "${find_args[@]}" | sort) # Execute find command using safe array expansion

# Optionally report errors to stderr if any occurred
if [ $error_count -gt 0 ]; then
    echo "Warning: Encountered $error_count errors reading file contents during context generation." >&2
    # Exit with a non-zero status to indicate partial success/warning
    exit 1
fi

# Exit silently on success
exit 0
