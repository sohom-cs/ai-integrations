# Shared by every script. CliFiles puts it, and end_of_input, before each one.
# This host's boot ID. A pid recorded under another boot ID is from before a
# restart, so it may now belong to another process.
boot=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo unknown)
# Record the CLI's shell pid $1 and its process group $2 in directory $3.
# Written aside and moved into place, so no reader sees half of it.
write_pid() { echo "$1 $2 $boot" > "$3/pid.new" && mv "$3/pid.new" "$3/pid"; }
# Read the pid and process group from directory $1 into $pid and $group. Fails
# if there are none, or if they're from before this host restarted.
read_pid() { { read -r pid group launched < "$1/pid"; } 2>/dev/null && [ "$launched" = "$boot" ]; }
