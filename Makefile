PLUGIN_VERSION=0.0.1
PLUGIN_ID=candlestick-chart

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip resource plugin.json webapps
