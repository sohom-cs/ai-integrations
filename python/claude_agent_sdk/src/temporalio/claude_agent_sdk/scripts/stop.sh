# Stop the CLI and everything it started, then remove its files in directory
# $1. As in the Claude Agent SDK: end its stdin and wait up to 5 seconds for it
# to exit, then send TERM to its process group and wait up to 5 seconds, then
# send KILL.
dir=$1
# Wait up to 5 seconds, while the command in the arguments keeps succeeding.
wait_while() {
  i=0
  while "$@" && [ $i -lt 50 ]; do
    sleep 0.1
    i=$((i + 1))
  done
}
running() { [ ! -e "$dir/exit" ] && kill -0 "$pid" 2>/dev/null; }
if read_pid "$dir"; then
  touch "$dir/stdin/end" 2>/dev/null
  wait_while running
  # A negative pid is the process group. dash's kill takes no "--".
  if kill -TERM "-$group" 2>/dev/null; then
    wait_while kill -0 "-$group" 2>/dev/null
    kill -KILL "-$group" 2>/dev/null
  fi
fi
rm -rf "$dir"
