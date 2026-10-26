# Print one of: the CLI's exit status; "running" if it may still write output;
# or "lost" if it stopped without an exit status, such as when it was killed or
# its host restarted.
dir=$1
if [ -e "$dir/exit" ]; then
  cat "$dir/exit"
elif read_pid "$dir" && kill -0 "$pid" 2>/dev/null; then
  echo running
else
  echo lost
fi
