# Start the CLI once, detached, with its files in directory $1: run $2
# (run_cli.sh) with the arguments after it.
# - mkdir is atomic, so only one attempt creates the stdin directory and starts
#   the CLI. A retry, even a concurrent one, starts no second CLI.
# - The stdout and stderr files exist before this returns, so watching them can
#   start at once.
# - The CLI's output goes to files, so this returns without waiting on it.
# - The caller runs this in a process group of its own, so $$ is that group.
#   stop.sh signals the group to stop the CLI and everything it started.
dir=$1 run=$2
shift 2
mkdir -p -m 700 "$dir" && : >>"$dir/stdout.jsonl" && : >>"$dir/stderr.log" || exit 1
if mkdir "$dir/stdin" 2>/dev/null; then
  sh -c "$run" sh "$dir/stdin" "$dir/exit" "$@" >>"$dir/stdout.jsonl" 2>>"$dir/stderr.log" &
  write_pid $! $$ "$dir"
fi
