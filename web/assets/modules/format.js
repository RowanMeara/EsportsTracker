/**
 * Adds separators to large numbers.
 *
 * Example: 1000 => '1,000'.
 *
 * @param n, the number to format.
 * @param separator, what to seperate with.
 * @returns {string}
 */
function splitMille (n, separator = ',') {
  let num = (n + '')
  let decimals = ''
  if (/\./.test(num)) {
    decimals = num.replace(/^.*(\..*)$/, '$1')
  }
  num = num.replace(decimals, '')
    .split('').reverse().join('')
    .match(/[0-9]{1,3}-?/g)
    .join(separator).split('').reverse().join('')

  return `${num}${decimals}`
}

/**
 * Converts a large number to a string represented in millions.
 *
 * @param num, the number to format
 * @returns {string}
 */
function formatMillions (num) {
  num = num / 1000000
  return num.toFixed(1) + 'M'
}

function formatPercent (num, decimals = 1) {
  num = num*100
  return num.toFixed(decimals) + '%'
}

module.exports = {
  splitMille: splitMille,
  formatMillions: formatMillions,
  formatPercent: formatPercent
}