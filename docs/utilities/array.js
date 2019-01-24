/**
 * Groups an array of objects by property.
 *
 * @param {TValue[]} list
 * An array.
 *
 * @param {string} key
 * The name of a property to use as a key.
 *
 * @returns {Map<TKey, TValue[]>}
 * A map of keys to groups.
 *
 * @template TKey, TValue
 */
function groupBy (list, key) {
  return list.reduce((map, item) => {
    const k = item[key]
    if (!map.has(item[key])) {
      map.set(k, [])
    }

    map.get(k).push(item)
    return map
  }, new Map())
}

/**
 * Separates items in an array.
 *
 * @param {T[]} list
 * An array.
 *
 * @param {U} separator
 * An item to insert between all elements in the array.
 *
 * @returns {(T|U)[]}
 * An array.
 *
 * @template T, U
 */
function join (list, separator) {
  return list.reduce((result, item, index, array) => {
    result.push(item)

    if (index < array.length - 1) {
      result.push(separator)
    }

    return result
  }, [])
}

module.exports = {
  groupBy, join
}
