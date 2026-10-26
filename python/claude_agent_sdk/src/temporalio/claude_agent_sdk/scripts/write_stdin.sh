# Write this command's stdin as the CLI's stdin file $2, in directory $1. A
# retry writes the same file again. Written aside and moved into place, so the
# CLI never reads part of it.
dir=$1 number=$2
aside=$(mktemp "$dir/stdin/.XXXXXX") && cat > "$aside" && mv "$aside" "$dir/stdin/$number.jsonl"
