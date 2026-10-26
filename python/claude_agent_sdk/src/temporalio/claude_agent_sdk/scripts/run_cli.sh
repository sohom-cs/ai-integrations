# Run the CLI, the arguments after $2, feeding its stdin from the files in
# directory $1 in order: 1.jsonl, 2.jsonl, and so on. Each file appears whole.
# Feeding stops, which closes the CLI's stdin, at a line equal to $end_of_input
# (set by CliFiles), or once stop.sh creates the "end" file and every numbered
# file was fed. When the CLI exits, write its status to $exited, which also
# stops the feeding. The status is written aside and moved into place, so no
# reader sees it empty.
stdin=$1 exited=$2
shift 2
n=1
while [ ! -e "$exited" ]; do
  if [ -e "$stdin/$n.jsonl" ]; then
    while IFS= read -r line; do
      [ "$line" = "$end_of_input" ] && exit
      printf '%s\n' "$line"
    done < "$stdin/$n.jsonl"
    n=$((n + 1))
  else
    [ -e "$stdin/end" ] && exit
    sleep 0.05
  fi
done | { "$@"; echo $? > "$exited.new" && mv "$exited.new" "$exited"; }
