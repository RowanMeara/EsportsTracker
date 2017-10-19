import 'jquery'
import './loadCharts.js'
import './googleCharts.js'
import 'bootstrap/dist/js/bootstrap.js'
import '../stylesheets/style.sass'
import './format.js'

/**
 * Webpack will not bundle files together unless they are required by every entrypoint
 * even if it substantially increases the amount of javascript that needs to be downloaded
 * to look at just two pages.  Importing all of the large shared modules here ensures that
 * webpack bundles them into a commons.js file and saves us from repeating this list in
 * every entrypoint.
 */
