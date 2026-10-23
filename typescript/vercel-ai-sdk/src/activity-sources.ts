/**
 * Lets the plugin refuse an Activity name that is already taken. The SDK's
 * plugin merge (`{...existing, ...param}`) keeps the last Activity of each
 * name, so without this check a plugin Activity silently replaces a different
 * Activity with the same name. @internal
 */

/**
 * What each plugin Activity depends on. Two Activities with the same name and
 * the same sources do the same thing, so one may replace the other.
 */
const activitySources = new WeakMap<object, readonly unknown[]>();

/** Records `sources` for every Activity in `activities` and returns it. */
export function withSources<T extends object>(activities: T, sources: readonly unknown[]): T {
  for (const fn of Object.values(activities)) activitySources.set(fn, sources);
  return activities;
}

/**
 * Throws if an Activity in `added` would replace a different Activity with the
 * same name in `existing`. The same function, or one built with the same
 * sources, may replace itself: that is the same plugin instance, or one with
 * the same options, registered twice (e.g. on the NativeConnection and the Worker).
 */
export function refuseReplacedActivities(plugin: string, existing: object | undefined, added: object): void {
  // What the Worker registers: own enumerable keys with function values.
  const registered = new Map(Object.entries(existing ?? {}).filter(([, fn]) => typeof fn === 'function'));
  for (const [name, fn] of Object.entries(added)) {
    const other = registered.get(name);
    if (other === undefined || other === fn || sameSources(other, fn)) continue;
    throw new TypeError(
      `Duplicate registration of Activity '${name}': ${plugin} would replace a different Activity with this name. ` +
        `Rename or remove the other registration, or give each ${plugin} on this Worker the same options.`
    );
  }
}

function sameSources(a: object, b: object): boolean {
  const x = activitySources.get(a);
  const y = activitySources.get(b);
  return x !== undefined && y !== undefined && x.length === y.length && x.every((v, i) => v === y[i]);
}
