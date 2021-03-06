// Primary map styling created with Google Maps
// Styling Wizard at https://mapstyle.withgoogle.com/
var mapstyle = [
    {
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#f5f5f5"
        }
      ]
    },
    {
      "elementType": "labels.icon",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#616161"
        }
      ]
    },
    {
      "elementType": "labels.text.stroke",
      "stylers": [
        {
          "color": "#f5f5f5"
        }
      ]
    },
    {
      "featureType": "administrative",
      "elementType": "geometry",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "administrative.land_parcel",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#bdbdbd"
        }
      ]
    },
    {
      "featureType": "administrative.neighborhood",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "poi",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "poi",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#eeeeee"
        }
      ]
    },
    {
      "featureType": "poi",
      "elementType": "labels.text",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "poi",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#757575"
        }
      ]
    },
    {
      "featureType": "poi.park",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#e5e5e5"
        }
      ]
    },
    {
      "featureType": "poi.park",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#9e9e9e"
        }
      ]
    },
    {
      "featureType": "road",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#ffffff"
        }
      ]
    },
    {
      "featureType": "road",
      "elementType": "labels",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "road",
      "elementType": "labels.icon",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "road.arterial",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "road.arterial",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#757575"
        }
      ]
    },
    {
      "featureType": "road.highway",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#dadada"
        }
      ]
    },
    {
      "featureType": "road.highway",
      "elementType": "labels",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "road.highway",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#616161"
        }
      ]
    },
    {
      "featureType": "road.local",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "road.local",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#9e9e9e"
        }
      ]
    },
    {
      "featureType": "transit",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "transit.line",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#e5e5e5"
        }
      ]
    },
    {
      "featureType": "transit.station",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#eeeeee"
        }
      ]
    },
    {
      "featureType": "water",
      "elementType": "geometry",
      "stylers": [
        {
          "color": "#c9c9c9"
        }
      ]
    },
    {
      "featureType": "water",
      "elementType": "labels.text",
      "stylers": [
        {
          "visibility": "off"
        }
      ]
    },
    {
      "featureType": "water",
      "elementType": "labels.text.fill",
      "stylers": [
        {
          "color": "#9e9e9e"
        }
      ]
    }
  ];

//GENERAL NAVIGATION AND ORCHESTRATION

function updateParams(param, value, mode){

  var url_vars = getUrlVars(param);

  switch (mode){
    case 'add':
      url_vars.push(value);
      break;
    case 'remove':
      const index = url_vars.indexOf(value);
      if (index > -1) {
          url_vars.splice(index, 1);
      }
      break;
    case 'replace':
      url_vars = [value]
  }
  
  //update with new array
  const url = new URL(window.location);
  url.searchParams.delete(param);
  for (const url_var of url_vars){
      url.searchParams.append(param, url_var);
  }
  window.history.pushState({}, '', url);
}

function removePlace(place_id){
    for (layer in layers_dict){
        if (layers_dict[layer]['place_id']==place_id){
            layers_dict[layer]['layer'].setMap(null)
            delete layers_dict[layer]
        }
    }

    //remove marker 
    place_details[place_id]['place_marker'].setMap(null)

    //if no rx layers left, also empty the geo layer  
    if (Object.keys(layers_dict).length==0){
        geo_layer.forEach(function(feature) {
        geo_layer.remove(feature);
        });
        flashZone.setMap(null)
        flashMarker.setMap(null)
        infowindow_resto.close()
        closeFlash()
        $('#flash').hide()
    }
        
    //reset the layer selector
    layers = new Set(Object.values(layers_dict).map(({vendor})=>vendor))
    layer_controller(layers, map)

    //remove from place_details
    delete place_details[place_id]
}

function addPlace(place_id){
    //add to place_details list
    place = {place_id:place_id}
    place_details[place_id] = place
    $('#flash').show()
    placeBoundaries([place_id], map, start, end)
}


function processPoints(geometry, callback, thisArg) {
  if (geometry instanceof google.maps.LatLng) {
    callback.call(thisArg, geometry);
  } else if (geometry instanceof google.maps.Data.Point) {
    callback.call(thisArg, geometry.get());
  } else {
    geometry.getArray().forEach(function(g) {
      processPoints(g, callback, thisArg);
    });
  }
}

function polygon_paths_from_bounds(bounds, clockwise){
  var path = new google.maps.MVCArray();
  var ne = bounds.getNorthEast();
  var sw = bounds.getSouthWest();
  if (clockwise) {
    path.push(ne);
    path.push(new google.maps.LatLng(sw.lat(), ne.lng()));
    path.push(sw);
    path.push(new google.maps.LatLng(ne.lat(), sw.lng()));
  }
  else{
    path.push(new google.maps.LatLng(ne.lat(), sw.lng()));
    path.push(sw);
    path.push(new google.maps.LatLng(sw.lat(), ne.lng()));
    path.push(ne);
  }
  return path
}

function getUrlVars(var_name){
  var vars = [], hash;
  var hashes = window.location.href.slice(window.location.href.indexOf('?') + 1).split('&');
  for(var i = 0; i < hashes.length; i++){
    hash = hashes[i].split('='); 
    if (hash[0]==var_name){                       
      vars.push(hash[1]);
    }
  }
  return vars;
}

function loadTab(tab=tab_name) {

  //highlight the selected tab
  tabs = document.getElementsByClassName("tab");
  for (i = 0; i < tabs.length; i++) {
    tabs[i].className = tabs[i].className.replace(" active", "");
  }
  document.getElementById(tab+"_tab").className += " active";

  var url = new URL(window.location);
  //load the maps and display the elements
  console.log(tab)
  switch (tab) {
      case 'resto':
          url.searchParams.delete('chain');
          resto_map = initMap('resto_map')    
          bounds = new google.maps.LatLngBounds()
          geo_layer = new google.maps.Data({map: resto_map});
          infowindow_resto = new google.maps.InfoWindow();
          flashZone = new google.maps.Polygon()
          flashMarker = new google.maps.Marker()
          closeMarker = new google.maps.Marker()
          flashed = false

          $('.scene').hide()
          $('#resto_scene').show()

          place_ids = Object.keys(place_details)
          if (place_ids.length !== 0){
            placeBoundaries(place_ids, resto_map, start, end)
            $('#flash').show()
          }

          break;
      case 'country':
          url.searchParams.delete('place_id');
          url.searchParams.delete('chain');
          window.history.pushState({}, '', url);

          infowindow_country = new google.maps.InfoWindow();
          bounds = new google.maps.LatLngBounds();
          coverage_bounds =  new google.maps.LatLngBounds();
          layer_bounds = new google.maps.LatLngBounds();
          country_map = initMap('country_map')
          $('.scene').hide()
          $('#country_scene').show()
          country_map = countryMap(start, end, country_map);
          break;
      case 'chains':
          url.searchParams.delete('place_id');
          chains_map = initMap('chains_map')    
          bounds = new google.maps.LatLngBounds()
          infowindow_chains = new google.maps.InfoWindow();
          infowindow_chains_place = new google.maps.InfoWindow();

          $('.scene').hide()
          $('#chains_scene').show()
          console.log(chain)

          chains_map.addListener('zoom_changed',function (event) {
            var timer;
            return function() {
                console.log('changezoom')
                clearTimeout(timer);
                timer = setTimeout(function() {
                  if (chain !== 'None' & chain !== null ){
                    loadChainMarkers();
                  }
                  zoom = chains_map.getZoom()
                  for (vendor in chains_dict){
                    chains_dict[vendor]['layer'].setStyle({
                      fillColor: chains_dict[vendor]['layer'].style.fillColor,
                      fillOpacity: 0.5,
                      strokeColor: 1,
                      strokeWeight: zoom/4,
                      zIndex: chains_dict[vendor]['layer'].style.zIndex
                    });
                  }
                }, 500);
            }
          }());

          chains_map.addListener('bounds_changed',function (event) {
            var timer;
            return function() {
                clearTimeout(timer);
                timer = setTimeout(function() {
                  if (chain !== 'None' & chain !== null ){
                    loadChainMarkers();
                  }
                }, 500);
            }
          }());
          
          if (chain !== 'None' & chain !== null ){
            loadChains(chain, start, end)
          }
          break;

  }
}

function initMap(map_id) {  
  map = new google.maps.Map(document.getElementById(map_id), {
      zoom: 7,
      mapTypeControl: false,
      streetViewControl: false,
      fullscreenControl: false,
      center: { lat: 53.532, lng: -1.128 },
      styles: mapstyle,
      maxZoom: 16
  });
  return map
}

function setVisible(selector, visible) {
  document.querySelector(selector).style.display = visible ? 'block' : 'none';
}

///RESTO TAB

function closeFlash(){
  flashMarker.setMap(null);
  closeMarker.setMap(null);
  flashZone.setMap(null);
  $('#datemask').hide()
  $('.reportrange').show()
  $('#flash').show()
  $('.token-search').show()
  $('.tokenize>.tokens-container').css('background-color', '')
  if (flashed){
    placeBoundaries(Object.keys(place_details), map, start, end)   
  }
  flashed = false
  map.controls[google.maps.ControlPosition.BOTTOM_CENTER].clear()
}

function flashMessage(text){
  flashLabel = document.createElement('div');
  flashLabel.setAttribute("id", "flashLabel");
  flashLabel.innerHTML = text
  map.controls[google.maps.ControlPosition.BOTTOM_CENTER].clear()
  map.controls[google.maps.ControlPosition.BOTTOM_CENTER].push(flashLabel);
}

function setupFlash(){

  if (typeof flashZone != "undefined") {
    flashZone.setMap(null)
  }
  $('#flash').hide()
  $('.reportrange').hide()
  $('#datemask').show()
  $('.token-search').hide()
  $('.tokenize>.tokens-container').css('background-color', '#f1f1f1')

  flashMessage('Adjust the area to scrape, and then click')

  //get the bounding box around the currentlty loaded
  flash_bounds = new google.maps.LatLngBounds();
  for (rx_layer in layers_dict){        
    layers_dict[rx_layer]['layer'].forEach(function(feature){
       feature.getGeometry().forEachLatLng(function(latlng){
           flash_bounds.extend(latlng)
           });
        });
  }

  var paths = new google.maps.MVCArray();
  paths.push(polygon_paths_from_bounds(map.getBounds(), true))
  paths.push(polygon_paths_from_bounds(flash_bounds, false))

  flashZone = new google.maps.Polygon({
    paths: paths,
    strokeColor: 'grey',
    strokeOpacity: 0.8,
    strokeWeight: 2,
    fillColor: 'grey',
    fillOpacity: 0.8,
    editable: true
  });

  flashZone.setMap(resto_map);

  function flashZoneChanged(flashZone){
    flash_paths =  flashZone.getPaths().getArray()[1]
    top_point = {
      'lng' : null,
      'lat' : null
    }
    flash_bounds = []
    inner_bounds = new google.maps.LatLngBounds()
    flash_paths.getArray().forEach(function(value, index, array_x) {
      inner_bounds =  inner_bounds.extend(value)
      point = value.toJSON()
      if ((point['lat']>top_point['lat']) || (point['lng']>top_point['lng'])){
        top_point = point
      }
      flash_bounds.push([point['lng'],point['lat']])
    })
    flash_bounds.push(flash_bounds[0]) //close the polygon

    place_ids_to_scrape = Object.keys(place_details)

    //flash icon
    flashMarker.setMap(null);
    var icon = {
      url: flash_icon,
      scaledSize: new google.maps.Size(100, 200), // scaled size
      origin: new google.maps.Point(0,0), // origin
      anchor: new google.maps.Point(50, 100), // anchor
    };
    flashMarker = new google.maps.Marker({
      position: inner_bounds.getCenter(),
      map: resto_map,
      icon: icon, 
      opacity: 0.7,
      label: {
        text: 'Click to Scrape',
        color: 'black',
        fontSize: "20px"
      }
    });
    google.maps.event.addListener(flashMarker, 'click', function() {
      triggerFlash();
    })
    google.maps.event.addListener(flashMarker, 'mouseover', function() {
      flashMarker.setOpacity(1)
    })
    google.maps.event.addListener(flashMarker, 'mouseout', function() {
      flashMarker.setOpacity(0.5)
    })
    
    //close icon
    closeMarker.setMap(null);
    var icon = {
      url: close_icon,
      scaledSize: new google.maps.Size(30, 30), // scaled size
      origin: new google.maps.Point(0,0), // origin
      anchor: new google.maps.Point(0, 40), // anchor
      opacity: 0
    };
    closeMarker = new google.maps.Marker({
      position: top_point,
      map: resto_map,
      icon: icon,
    });
    google.maps.event.addListener(closeMarker, 'click', function() {
      closeFlash();
    })

    google.maps.event.addListener(resto_map, 'bounds_changed', function(){
      if (flashZone.getMap() !== null){
        paths = flashZone.getPaths()
        flash_bounds = resto_map.getBounds()
        paths.setAt(0, polygon_paths_from_bounds(flash_bounds, true))
      }
    })

    //flashLabel = new google.maps.Marker({
    //    position: inner_bounds.getCenter(),
    //   label: {
    //      text: 'Click to Scrape',
    //      color: 'black',
    //      fontSize: "20px"
    //    },
    //    map: resto_map, 
    //    icon: " "
    //  });

    flashZone.flash_bounds = flash_bounds
    flashZone.place_ids_to_scrape = place_ids_to_scrape

    return flashZone
  }

  flashZone = flashZoneChanged(flashZone);

  flashZone.getPaths().forEach(function(path, index){
    google.maps.event.addListener(path, 'insert_at', function(){ 
      flashMessage('Click to start scrape')
      flashZone = flashZoneChanged(flashZone)
    });
    google.maps.event.addListener(path, 'remove_at', function(){
      flashMessage('Click to start scrape')
      flashZone = flashZoneChanged(flashZone)
    });
    google.maps.event.addListener(path, 'set_at', function(){
      flashMessage('Click to start scrape')
      flashZone = flashZoneChanged(flashZone)
    });
  });
}

function triggerFlash(){

  setVisible('#loading', true);
  flashMarker.setMap(null);
  flashMessage('Starting scrape...')
  var xhr = new XMLHttpRequest();

  xhr.open("POST", '/flash');
  xhr.setRequestHeader("Content-Type", "application/json");

  message = JSON.stringify({
    'place_ids': Array.from(flashZone.place_ids_to_scrape),
    'bounds':Array.from(flashZone.flash_bounds)
    })

  xhr.send(message);
  var position = 0;

  function handleNewData() {
      var messages = xhr.responseText.split('\n');
      messages.slice(position, -1).forEach(function(value) {
        flashed = true
        $('.reportrange span').html('Real Time')
        json = JSON.parse(value)
        if (json['status']=='OK'){
          render_places(json['flash_result'], mode='dynamic')
          number_postcodes_scraped = json['postcodes_scraped'].length
          number_postcodes_to_scrape = json['postcodes_to_scrape'].length
          flashMessage(number_postcodes_scraped.toString()+'/'+number_postcodes_to_scrape.toString()+' postcodes scraped')
        }
        else{
          switch(json['message']) {
            case 'TOO MANY SECTORS':
              flashMessage('Area too large to scrape.. adjust to include fewer postcodes')
              break;
            case 'TOO FEW SECTORS':
              flashMessage('Area too small... adjust to add more postcodes')
              break;
            case 'TOO FEW PLACES':
              flashMessage('Adjust the area to include at least one restaurant')
              break;
            default:
              flashMessage('Sorry, something went wrong!')
          }
        } 
      });
      position = messages.length - 1;
  }

  var timer;
  timer = setInterval(function() {
      // check the response for new data
      handleNewData();
      // stop checking once the response has ended
      if (xhr.readyState == XMLHttpRequest.DONE) {
          clearInterval(timer);
          setVisible('#loading', false);
          console.log("DONE")
      }
  }, 1000);
}


function restoFlashOld(){
  setVisible('#flash_loading', true);

  bounds =  flashZone.getBounds()
  var ne = bounds.getNorthEast();
  var sw = bounds.getSouthWest();

  place_ids = new Set();
  vendors = new Set();  

  for (const [key, value] of Object.entries(place_details)) {
    place_ids.add(key)
    for (const [inner_key, inner_value] of Object.entries(place_details[key]['entities'])) {  
      vendors.add(inner_value['vendor'])
    }
  }

  var place_string = "&place_id="+Array.from(place_ids).join("&place_id=")
  var vendor_string = "&vendors="+Array.from(vendors).join("&vendors=")

  $.ajax({
    async: true,
    type: "GET",
    url: "/flash?lngw="+sw.lng()+"&lats="+sw.lat()+"&lnge="+ne.lng()+"&latn="+ne.lat()+place_string+vendor_string,
    success: function (response) {
      flashZone.setMap(null)
      flashMarker.setMap(null)
      render_places(response)
      $('.reportrange span').html('Just Now')
      setVisible('#flash_loading', false);
    }
  });
}

function placeBoundaries(place_ids, map, start, end) {

  //ensure the datepicker shows the correct date (after a flash)
  $('.reportrange span').html(start.format('MMMM D, YYYY') + ' - ' + end.format('MMMM D, YYYY'));

  function restoGeolayer(map){

    function createInfoWindow(map, event, infowindow){
      var name = event.feature.getProperty('postcode_sector');
      var population = event.feature.getProperty('population');

      contentString = document.createElement("div");
      table = document.createElement("table");
      table.classList.add('GeoPopCoverage')
      table_row = document.createElement("tr")
      table_cell = document.createElement("th")
      table_cell.innerHTML = name
      table_cell.colSpan = 2
      table_row.appendChild(table_cell)
      table.appendChild(table_row)
      table_row = document.createElement("tr")
      table_cell = document.createElement("td")
      table_cell.innerHTML = 'Population'
      table_row.appendChild(table_cell)
      table_cell = document.createElement("td")
      table_cell.innerHTML = population.toLocaleString()
      table_row.appendChild(table_cell)
      table.appendChild(table_row)
      contentString.appendChild(table)

      var info_bounds = new google.maps.LatLngBounds();
      var geometry = event.feature.getGeometry();
    
      geometry.forEachLatLng(function(point){
        info_bounds.extend({
          lat : point.lat(),
          lng : point.lng()
        });
      });
      var center = info_bounds.getCenter();
    
      // Create invisible marker for info window
      var marker = new google.maps.Marker({
        position: center,
        map: map,
        visible : false
      });
      // Create info window
      infowindow.setContent(contentString);
      infowindow.open(map, marker);
    }
  
    //remove any extant features
    geo_layer.forEach(function(feature) {
      geo_layer.remove(feature);
    });
  
    //fill the map with the geo objects when the bound are settled, if zoom is >=10
    bounds =  map.getBounds()
    var ne = bounds.getNorthEast();
    var sw = bounds.getSouthWest();
    geo_layer.loadGeoJson('geo_objects.json?lngw='+sw.lng()+'&lats='+sw.lat()+'&lnge='+ne.lng()+'&latn='+ne.lat(), null, function (features) {
      console.log('geo objects loaded')
    });
  
    geo_layer.addListener('click', function(event) {
      createInfoWindow(map, event, infowindow_resto); 
    });
    
    geo_layer.setStyle(function(feature) {
      return /** @type {!google.maps.Data.StyleOptions} */({
        fillOpacity: 0,
        strokeColor: 'gray',
        strokeWeight: 0.5,
        zIndex: 1000
      });
    });
  
    return map
  }

  this.layer_controller = function (layers, map){

    function LayerControl(controlDiv, map, layers) {
      layersDataTable = document.createElement("table");
      layers.forEach(function(item, index, array) {
        layersDataRow = document.createElement("tr");
        layersDataCell = document.createElement("td");
        // Build the checkboxes and labels
        const controlUI = document.createElement("input");
        controlUI.type="checkbox";
        controlUI.checked=true;
        controlUI.id=item;
        controlUI.zoom = 3.5;
        layersDataCell.appendChild(controlUI);
        const labelUI = document.createElement("label");
        labelUI.for=item;
        labelUI.innerHTML=vendor_data[item]['vendor_name'];
        labelUI.style.color = vendor_data[item]['vendor_colour']//"rgb(25,25,25)";
        labelUI.style.fontFamily = "Roboto,Arial,sans-serif";
        labelUI.style.fontSize = "18px";
        labelUI.style.lineHeight = "25px";
        labelUI.style.paddingLeft = "10px";
  
        layersDataCell.appendChild(labelUI);
        layersDataRow.appendChild(layersDataCell)
        // Setup the click event listener
        controlUI.addEventListener("click", function(e) {
            toggle_layer(controlUI.id, controlUI.checked)
        });
        layersDataTable.appendChild(layersDataRow)
      });
      controlDiv.appendChild(layersDataTable)
    }
  
    function toggle_layer(vendor, state){
      for (const [key, value] of Object.entries(layers_dict)) {
        if (value['vendor']==vendor){
          if(state){
            layers_dict[key]['layer'].setMap(map)
          } else {
            layers_dict[key]['layer'].setMap(null)
          }
        }
      }
    }
  
    map.controls[google.maps.ControlPosition.TOP_RIGHT].clear()
    if (layers.size>0){
      const layerControlDiv = document.createElement("div");
      layerControlDiv.style.backgroundColor = "#fff";
      layerControlDiv.style.border = "2px solid #fff";
      layerControlDiv.style.borderRadius = "3px";
      layerControlDiv.style.boxShadow = "0 2px 6px rgba(0,0,0,.3)";
      layerControlDiv.style.cursor = "pointer";
      layerControlDiv.style.marginTop = "10px";
      layerControlDiv.style.marginRight = "10px";
      layerControlDiv.style.textAlign = "left";
      layerControlDiv.style.padding = "10px";
      LayerControl(layerControlDiv, map, layers);
      map.controls[google.maps.ControlPosition.TOP_RIGHT].push(layerControlDiv);
    }
  }

  //function render_places(json){
  this.render_places = function(json, mode='static'){

    place_ids = Object.keys(json)
    place_ids.forEach(function (place_id, index) {
      //remove any existing markers
      if (place_details[place_id] !== undefined){
        if (place_details[place_id]['place_marker'] !== undefined){
          place_details[place_id]['place_marker'].setMap(null)
        }
      }

      //update the place details
      places_loaded.push(place_id)
      place_details[place_id] = json[place_id]['place_details']

      //remove any existing layers for this place_id
      for (layer in layers_dict){
        if (layers_dict[layer]['place_id']==place_id){
          console.log("removing")
          layers_dict[layer]['layer'].setMap(null)
          delete layers_dict[layer]
        }
      }

      for (row in json[place_id]['place_map']['features']){
        if (json[place_id]['place_map']['features'][row]['geometry'] !== null){
          vendor = json[place_id]['place_map']['features'][[row]]['properties']['vendor'];
          place_vendor_id = json[place_id]['place_map']['features'][[row]]['properties']['place_vendor_id'];
          delivery_area = json[place_id]['place_map']['features'][[row]]['properties']['delivery_area'];
          delivery_population = json[place_id]['place_map']['features'][[row]]['properties']['delivery_population'];

          rx_layer = new google.maps.Data({map: map});
          rx_layer.addGeoJson(json[place_id]['place_map']['features'][row]);

          rx_layer.setStyle({
            fillColor: vendor_data[vendor]['vendor_colour'],
            fillOpacity: 0.5,
            strokeColor: 1,
            strokeWeight: 3,
            zIndex: 1/delivery_area
          });

          layers_dict[place_vendor_id] = {'layer':rx_layer, 'place_id':place_id, 'vendor':vendor, 'delivery_area':delivery_area, 'delivery_population': delivery_population}
        }
      }

      //add a place marker
      if (Object.values(layers_dict).map(({place_id})=>place_id).includes(place_id)){
        label_text = json[place_id]['place_details']['place_name']
      } 
      else{
        label_text = json[place_id]['place_details']['place_name'] + ' (No Data)'
      }

      marker_latlng = new google.maps.LatLng(json[place_id]['place_details']['place_lat'], json[place_id]['place_details']['place_lng'])
      place_marker = new google.maps.Marker({
        place_id: place_id,
        position: marker_latlng,
        zIndex: 2000,
        label: {
          text: label_text,
          color: 'black',
          fontSize: '15px',
          fontWeight: 'bold',
          width: '60px'
        },
        map: map,
        icon: {
          url: rx_icon,
          labelOrigin: new google.maps.Point(25, 50),
        }
      });
      place_details[place_id]['place_marker'] = place_marker

      //add the click infowindow
      google.maps.event.addListener(place_marker, 'click', function() {
        contentString = document.createElement("div");
        table = document.createElement("table");
        table.classList.add('PlacePopCoverage')
        table_row = document.createElement("tr")
        table_cell = document.createElement("th")
        table_cell.innerHTML = 'Population Coverage'
        table_cell.colSpan = 2
        table_row.appendChild(table_cell)
        table.appendChild(table_row)
        
        for (layer in layers_dict){
          if (layers_dict[layer]['place_id']==this.place_id){
              table_row = document.createElement("tr")
              table_cell = document.createElement("td")
              table_cell.innerHTML = vendor_data[layers_dict[layer]['vendor']]['vendor_name']
              table_row.appendChild(table_cell)
              table_cell = document.createElement("td")
              table_cell.innerHTML = layers_dict[layer]['delivery_population'].toLocaleString()
              table_row.style.color = vendor_data[layers_dict[layer]['vendor']]['vendor_colour']
              table_row.appendChild(table_cell)
              table.appendChild(table_row)
          }
        }
        contentString.appendChild(table)
        if (infowindow_resto) {
          infowindow_resto.close();
        }

        infowindow_resto = new google.maps.InfoWindow({
          content: contentString,
        });
        infowindow_resto.open({
          anchor: this,
          map,
          shouldFocus: false,
        });
      })
    })

    //trigger the geolayer and the map key only when all are loaded
    if (mode=='static'){ //only do this if we're not in flash mode
      places_to_load = Object.keys(place_details)
      if (places_to_load.every(i => places_loaded.includes(i))){
        //fit map to layers on map
        layer_bounds = new google.maps.LatLngBounds();
        for (rx_layer in layers_dict){        
          layers_dict[rx_layer]['layer'].forEach(function(feature){
            feature.getGeometry().forEachLatLng(function(latlng){
                layer_bounds.extend(latlng)
                });
              });
        }

        layer_bounds.extend(marker_latlng);
        map.fitBounds(layer_bounds, 0)

        //add geoobjects
        if (map.getZoom()>=10){
          restoGeolayer(map);
        }

        layers = new Set(Object.values(layers_dict).map(({vendor})=>vendor))
        layer_controller(layers, map)
      }
    }
  }

  setVisible('#loading', true);
  
  $.getJSON('deliveryboundary.json?start='+ start.format('YYYY-MM-DD')+"&end="+ end.format('YYYY-MM-DD')+"&place_id="+place_ids.join("&place_id="), function (json) {
    try{
      render_places(json)
      setVisible('#loading', false);
    }
    catch(err){
      setVisible('#loading', false);
    }
  });   
  return 'done'
}

///COUNTRY COVERAGE MAP

function countryMap(start, end, map){

  function gethex(value, min_value, max_value) {
    if (value>min_value){
        value = Math.min(value, max_value)/max_value
        h = (1.0-value)*240
        s = 100
        l = 50  
        l /= 100;
        const a = s * Math.min(l, 1 - l) / 100;
        const f = n => {
        const k = (n + h / 30) % 12;
        const color = l - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
        return Math.round(255 * color).toString(16).padStart(2, '0');   // convert to Hex and prefix "0" if needed
        };
        return `#${f(0)}${f(8)}${f(4)}`;}
    else{
        return '#d3d3d3'
    }
  }

  this.CountryData = function(start, end, map) {
    setVisible('#loading', true);
    infowindow_country.close()
    
    if (Object.keys(coverage_layer).length !== 0){
      coverage_layer.forEach(function(feature) {
        coverage_layer.remove(feature);
      });
    }

    zoom = map.getZoom()
    bounds =  map.getBounds()
    var ne = bounds.getNorthEast();
    var sw = bounds.getSouthWest();

    if (zoom>=11){
      window.granularity='sectors'
    }
    else{
      window.granularity='districts'
    }

    $.getJSON('country.json?start='+ start.format('YYYY-MM-DD')+"&end="+ end.format('YYYY-MM-DD')+'&lngw='+sw.lng()+'&lats='+sw.lat()+'&lnge='+ne.lng()+'&latn='+ne.lat()+'&granularity='+granularity, function (json) {    
      coverage_layer = new google.maps.Data({map: map});
      coverage_layer.addGeoJson(json['coverage']);
      coverage_bounds =  map.getBounds()

      vendor = getUrlVars('vendor')[0]
      if (vendor == null) {
        vendor=default_vendor
      }
      colourMap(coverage_layer, vendor)

      //add the vendor selector
      country_controller(map, json['stats'])

        //listener for the infowindow
      coverage_layer.addListener('click', function(event) {
        createInfoWindow(map, event, infowindow_country); 
      });
      
      setVisible('#loading', false);
    });
    return map
  }

  function toggleDelivery(toggledElement){

    delivery_restaurant = document.getElementById('delivery_choice_restaurant');
    delivery_vendor = document.getElementById('delivery_choice_vendor');

    if (delivery_restaurant.checked == false & delivery_vendor.checked == false){
      if (toggledElement=='delivery_choice_restaurant'){
        delivery_vendor.checked=true
      }
      else if (toggledElement=='delivery_choice_vendor'){
        delivery_restaurant.checked=true
      }
    }

    if (delivery_restaurant.checked == true & delivery_vendor.checked == true){
      fulfillment_type = 'all'
    }
    else if (delivery_restaurant.checked == true & delivery_vendor.checked == false){
      fulfillment_type = 'restaurant'
    }
    else if (delivery_restaurant.checked == false & delivery_vendor.checked == true){
      fulfillment_type = 'vendor'
    }
    
    updateParams('delivery', fulfillment_type, 'replace')
    colourMap(coverage_layer, selected_vendor)
  }

  function colourMap(coverage_layer, vendor_name){  
    coverage_layer.setStyle(function(feature) {
      feature_stats = feature.getProperty('rx_counts')
      rx_count_vendor = 0
      if (fulfillment_type in feature_stats){
        if (vendor_name in feature_stats[fulfillment_type]){
          rx_count_vendor = feature_stats[fulfillment_type][vendor_name]
        }
      }

      color = gethex(rx_count_vendor,min_rx, max_rx)
      return /** @type {!google.maps.Data.StyleOptions} */({
        fillColor: color,
        strokeColor: color,
        strokeWeight: 0.5
      });
    });
  }

  function createLegend(){
    legend = document.createElement('div');
    legend.setAttribute("id", "legend");

    var div = document.createElement('center');
    div.innerHTML = '<h3>Restaurants<br/>Open<br/></h3>'
    legend.appendChild(div);
    steps = 10
    max_rx = 200 //params for the range of the legend
    min_rx = 1
    colors = Array.from({length: steps+1}, (x, i) => gethex(i*max_rx/steps, min_rx, max_rx));
    labels = Array.from({length: steps+1}, (x, i) => i*max_rx/steps);
    for (var key in colors) {
      var color = colors[key];
      var label = labels[key];
      var div = document.createElement('div');
      div.innerHTML = '<div class="cbox" style="background-color: '+ color + ';"><center>'+label+'</center>';
      legend.appendChild(div);
    };
    return legend
  };

  function country_controller(map, stats){

    function LayerControl(controlDiv, stats) {
      controlDiv.innerHTML = '';
      layersDataTable = document.createElement("table");
      layersDataTable.classList.add('GeoPopCoverage')
      layersDataRow = document.createElement("tr");
      layersDataCell = document.createElement("th");
      layersDataCell.innerHTML = ''
      layersDataRow.appendChild(layersDataCell)
      layersDataCell = document.createElement("th");
      layersDataCell.innerHTML = 'Number of<br>Restaurants';
      layersDataRow.appendChild(layersDataCell)
      layersDataCell = document.createElement("th");
      layersDataCell.innerHTML = 'Population<br>Coverage';
      layersDataRow.appendChild(layersDataCell)
      layersDataTable.appendChild(layersDataRow)

      selected_vendor = getUrlVars('vendor')[0]
      if (selected_vendor == null) {
        selected_vendor=default_vendor
      }

      for (var vendor in stats){
        layersDataRow = document.createElement("tr");
        layersDataCell = document.createElement("td");
        // Build the checkboxes and labels
        const controlUI = document.createElement("input");
        controlUI.type="radio";
        controlUI.name="vendor_choice"
        controlUI.checked=(selected_vendor==vendor);
        controlUI.id=vendor;
        controlUI.zoom = 3.5;
        layersDataCell.appendChild(controlUI);
        const labelUI = document.createElement("label");
        labelUI.for=vendor;
        labelUI.innerHTML=vendor_data[vendor]['vendor_name'];
        labelUI.style.color = vendor_data[vendor]['vendor_colour']//"rgb(25,25,25)";
        labelUI.style.fontFamily = "Roboto,Arial,sans-serif";
        labelUI.style.fontSize = "18px";
        labelUI.style.lineHeight = "25px";
        labelUI.style.paddingLeft = "10px";
        layersDataCell.appendChild(labelUI);
        layersDataCell.style.textAlign = 'left';
        layersDataRow.appendChild(layersDataCell)

        layersDataCell = document.createElement("td");
        labelDataCell = document.createElement("label");
        labelDataCell.innerHTML = stats[vendor][fulfillment_type]['rx_num'].toLocaleString()
        layersDataCell.appendChild(labelDataCell)
        layersDataRow.appendChild(layersDataCell)
        layersDataCell = document.createElement("td");
        labelDataCell = document.createElement("label");
        labelDataCell.innerHTML = stats[vendor][fulfillment_type]['delivery_population'].toLocaleString()
        layersDataCell.appendChild(labelDataCell)
        layersDataRow.appendChild(layersDataCell)
        // Setup the click event listener
        controlUI.addEventListener("click", function(e) {
          updateParams('vendor', controlUI.id, 'replace')
          selected_vendor = controlUI.id
          colourMap(coverage_layer, controlUI.id)
        });
        layersDataTable.appendChild(layersDataRow)
      }

      controlDiv.appendChild(layersDataTable)

      const hrule = document.createElement("hr");
      hrule.style.margin = 10;
      controlDiv.appendChild(hrule)

      layersDataTable = document.createElement("table");
      layersDataTable.classList.add('GeoPopCoverage')
      layersDataRow = document.createElement("tr");

      delivery_options = {'restaurant':'Restaurant','vendor':'Aggregator'}
      for (const [delivery, delivery_label] of Object.entries(delivery_options)){
        layersDataCell = document.createElement("td");
        const deliverytypeUI = document.createElement("input");
        deliverytypeUI.type="checkbox";
        deliverytypeUI.name="delivery_choice_"+delivery
        deliverytypeUI.checked=(fulfillment_type==delivery || fulfillment_type=='all');
        deliverytypeUI.id="delivery_choice_"+delivery;
        labelUI = document.createElement("label");
        labelUI.for='vendor';
        labelUI.innerHTML=delivery_label+'<br>delivered';
        labelUI.style.verticalAlign = 'middle';
        labelUI.style.textAlign = 'left';
        labelUI.style.marginLeft = 10;
        layersDataCell.appendChild(deliverytypeUI);
        layersDataCell.appendChild(labelUI);
        layersDataRow.appendChild(layersDataCell);
        deliverytypeUI.addEventListener("click", function(e) {
          toggleDelivery(deliverytypeUI.id);
          LayerControl(controlDiv, stats)
        });
      }
      layersDataTable.appendChild(layersDataRow);

      controlDiv.appendChild(layersDataTable)
    }
  
    map.controls[google.maps.ControlPosition.TOP_RIGHT].clear()
      const layerControlDiv = document.createElement("div");
      layerControlDiv.style.backgroundColor = "#fff";
      layerControlDiv.style.border = "2px solid #fff";
      layerControlDiv.style.borderRadius = "3px";
      layerControlDiv.style.boxShadow = "0 2px 6px rgba(0,0,0,.3)";
      layerControlDiv.style.cursor = "pointer";
      layerControlDiv.style.marginTop = "10px";
      layerControlDiv.style.marginRight = "10px";
      layerControlDiv.style.textAlign = "left";
      layerControlDiv.style.padding = "10px";
      LayerControl(layerControlDiv, stats);
      map.controls[google.maps.ControlPosition.TOP_RIGHT].push(layerControlDiv);
  }

  function createInfoWindow(map, event, infowindow){

    contentString = document.createElement("div");
    h = document.createElement("H2") 
    h.style.marginLeft = '10px'
    h.innerHTML = event.feature.getProperty('postcode_name')
    contentString.appendChild(h)

    table = document.createElement("table");
    table.classList.add('PlacePopCoverage')
    table_row = document.createElement("tr")
    table_cell = document.createElement("th")
    table_cell.innerHTML =  'Restaurants Open'
    table_cell.fontSize = 30;
    table_cell.colSpan = 2
    table_row.appendChild(table_cell)
    table.appendChild(table_row)
    
    feature_stats = event.feature.getProperty('rx_counts')
    vendor_open = 0;
    for (vendor in vendor_data){
      if (fulfillment_type in feature_stats){
        if (vendor in feature_stats[fulfillment_type]){
          vendor_open = feature_stats[fulfillment_type][vendor]
        }
      }

      table_row = document.createElement("tr")
      table_cell = document.createElement("td")
      table_cell.innerHTML = vendor_data[vendor]['vendor_name']
      table_row.appendChild(table_cell)
      table_cell = document.createElement("td")
      table_cell.innerHTML = vendor_open.toLocaleString()
      table_row.style.color = vendor_data[vendor]['vendor_colour']
      table_row.appendChild(table_cell)
      table.appendChild(table_row)
    }

    contentString.appendChild(table)

    var infowindow_bounds = new google.maps.LatLngBounds();
    var geometry = event.feature.getGeometry();
  
    geometry.forEachLatLng(function(point){
      infowindow_bounds.extend({
        lat : point.lat(),
        lng : point.lng()
      });
    });
    var center = infowindow_bounds.getCenter();
  
    // Create invisible marker for info window
    var marker = new google.maps.Marker({
      position: center,
      map: map,
      visible : false,
      zIndex: 9999
    });
    // Create info window
    infowindow.setContent(contentString);
    
    info_window_loading = true
    infowindow.open(map, marker);
    setTimeout(function(){ //block the redrawing of the map immediately after an infowindow is loaded
        info_window_loading = false; 
      }, 2000);
  }

  //wait for tiles to load then trigger the geodata load
  google.maps.event.addListener(map, 'idle', (function () {
    var timer;
    return function() {
        clearTimeout(timer);
        timer = setTimeout(function() {          
          if (!info_window_loading){
            coverage_layer_fills_map = (coverage_bounds.contains(country_map.getBounds().getNorthEast()) && coverage_bounds.contains(country_map.getBounds().getSouthWest())) 
            console.log(coverage_layer_fills_map)
            if (((country_map.getZoom()>=11 && granularity=='districts') || (country_map.getZoom()<=10 && granularity=='sectors')) || ((!coverage_layer_fills_map) && (granularity=='sectors'))){ 
              CountryData(start, end, map);
            }
          }
        }, 500);
    }
  }()));

  google.maps.event.addListenerOnce(map, 'idle', function(){
    map = CountryData(start, end, map)
  });

  //add the legend
  legend = createLegend()
  map.controls[google.maps.ControlPosition.LEFT_BOTTOM].push(legend);

  return map
}

////CHAINS TAB

function setChains(){
  chain = $('#chains_name').val();
  chain = chain.replace(/[\u2018\u2019]/g, "'"); //deals with issue where iOS has a different apostrophe

  zoom = chains_map.getZoom()
  bounds =  chains_map.getBounds()
  var ne = bounds.getNorthEast();
  var sw = bounds.getSouthWest();
  var lngw = sw.lng()
  var lats = sw.lat()
  var lnge = ne.lng()
  var latn = ne.lat()

  updateParams('chain', chain, 'replace')
  loadChains(chain, start, end)

  if (chains_map.getZoom()>10){
    loadChainMarkers(chain, lngw, lats, lnge, latn)
  }
  
}

//get markers
function createChainMarker(place){
  marker_latlng = new google.maps.LatLng(place['place_lat'], place['place_lng'])
  chains_marker = new google.maps.Marker({
    place_name: place['place_name'],
    place_id: place['place_id'],
    position: marker_latlng,
    map: chains_map,
    icon: {
      url: rx_icon,
      scaledSize: new google.maps.Size(25, 17)
    }
  });

  google.maps.event.addListener(chains_marker, 'click', function() {
    contentString = document.createElement("div");
    table = document.createElement("table");
    table.classList.add('PlacePopCoverage')
    table_row = document.createElement("tr")
    table_cell = document.createElement("th")
    table_cell.innerHTML = this.place_name
    table_cell.colSpan = 2
    table_row.appendChild(table_cell)
    table.appendChild(table_row)
    table_row = document.createElement("tr")
    table_cell = document.createElement("td")
    var place_link = document.createElement('a')
    place_link.setAttribute('href','/restaurant?place_id='+this.place_id)
    place_link.innerHTML = 'View restaurant'
    table_cell.appendChild(place_link)
    table_cell.colSpan = 2
    table_row.appendChild(table_cell)
    table.appendChild(table_row)

    contentString.appendChild(table)
    if (infowindow_chains_place) {
      infowindow_chains_place.close();
    }
    if (infowindow_chains) {
      infowindow_chains.close();
    }

    infowindow_chains_place = new google.maps.InfoWindow({
      content: contentString,
    });
    infowindow_chains_place.open({
      anchor: this,
      map,
      shouldFocus: false,
    });
  })


  return chains_marker
}

function loadChainMarkers(){

  var chain = $('#chains_name').val();
 
  if ((chains_map.getZoom()>=9) & (chain !== '')){
    bounds =  chains_map.getBounds()
    var ne = bounds.getNorthEast();
    var sw = bounds.getSouthWest();
    var lngw = sw.lng()
    var lats = sw.lat()
    var lnge = ne.lng()
    var latn = ne.lat()

    $.getJSON('/places.json?lngw='+lngw+'&lats='+lats+'&lnge='+lnge+'&latn='+latn+'&chain='+chain, function (json) {
      //remove current markers -- could be better to only remove ones that are not in new array      
      for (chains_marker in chainsMarkers){
          chainsMarkers[chains_marker].setMap(null)
      }
      chainsMarkers = []
      for (place in json){
        chains_marker = createChainMarker(json[place])
        chainsMarkers[json[place]['place_id']] = chains_marker        
      }
    })
  }
  else {
    //remove current markers
    for (chains_marker in chainsMarkers){
      chainsMarkers[chains_marker].setMap(null)
    }
    chainsMarkers = []
  }
}

function loadChains(chain, start, end) {
  setVisible('#loading', true);

  function decodeHtml(html) {
    var txt = document.createElement("textarea");
    txt.innerHTML = html;
    return txt.value;
  }
  $('#chains_name').val(decodeHtml(chain))

  //remove current layers
  for (chain_layer in chains_dict){        
    chains_dict[chain_layer]['layer'].setMap(null)
  }
  infowindow_chains.close()

  this.layer_controller = function (layers, map){

    function LayerControl(controlDiv, map, layers) {
      layersDataTable = document.createElement("table");
      layersDataTable.classList.add('GeoPopCoverage')
      layersDataRow = document.createElement("tr");
      layersDataCell = document.createElement("th");
      layersDataCell.innerHTML = ''
      layersDataRow.appendChild(layersDataCell)
      layersDataCell = document.createElement("th");
      layersDataCell.innerHTML = 'Number of<br>Restaurants';
      layersDataRow.appendChild(layersDataCell)
      layersDataCell = document.createElement("th");
      layersDataCell.innerHTML = 'Population<br>Coverage';
      layersDataRow.appendChild(layersDataCell)
      layersDataTable.appendChild(layersDataRow)

      layers.forEach(function(item, index, array) {
        layersDataRow = document.createElement("tr");
        layersDataCell = document.createElement("td");
        // Build the checkboxes and labels
        const controlUI = document.createElement("input");
        controlUI.type="checkbox";
        controlUI.checked=true;
        controlUI.id=item;
        controlUI.zoom = 3.5;
        layersDataCell.appendChild(controlUI);
        const labelUI = document.createElement("label");
        labelUI.for=item;
        labelUI.innerHTML=vendor_data[item]['vendor_name'];
        labelUI.style.color = vendor_data[item]['vendor_colour']//"rgb(25,25,25)";
        labelUI.style.fontFamily = "Roboto,Arial,sans-serif";
        labelUI.style.fontSize = "18px";
        labelUI.style.lineHeight = "25px";
        labelUI.style.paddingLeft = "10px";
        layersDataCell.appendChild(labelUI);
        layersDataCell.style.textAlign = 'left';
        layersDataRow.appendChild(layersDataCell)

        layersDataCell = document.createElement("td");
        labelDataCell = document.createElement("label");
        labelDataCell.innerHTML = chains_dict[item]['rx_num'].toLocaleString()
        layersDataCell.appendChild(labelDataCell)

        layersDataRow.appendChild(layersDataCell)
        layersDataCell = document.createElement("td");
        labelDataCell = document.createElement("label");
        labelDataCell.innerHTML = chains_dict[item]['delivery_population'].toLocaleString()
        layersDataCell.appendChild(labelDataCell)
        layersDataRow.appendChild(layersDataCell)
        // Setup the click event listener
        controlUI.addEventListener("click", function(e) {
            toggle_layer(controlUI.id, controlUI.checked)
        });
        layersDataTable.appendChild(layersDataRow)
      });
      controlDiv.appendChild(layersDataTable)
    }

    function toggle_layer(vendor, state){
      for (const [key, value] of Object.entries(chains_dict)) {
        if (value['vendor']==vendor){
          if(state){
            chains_dict[key]['layer'].setMap(map)
          } else {
            chains_dict[key]['layer'].setMap(null)
          }
        }
      }
    }
  
    map.controls[google.maps.ControlPosition.TOP_RIGHT].clear()
    if (layers.size>0){
      const layerControlDiv = document.createElement("div");
      layerControlDiv.style.backgroundColor = "#fff";
      layerControlDiv.style.border = "2px solid #fff";
      layerControlDiv.style.borderRadius = "3px";
      layerControlDiv.style.boxShadow = "0 2px 6px rgba(0,0,0,.3)";
      layerControlDiv.style.cursor = "pointer";
      layerControlDiv.style.marginTop = "10px";
      layerControlDiv.style.marginRight = "10px";
      layerControlDiv.style.textAlign = "left";
      layerControlDiv.style.padding = "10px";
      LayerControl(layerControlDiv, map, layers);
      map.controls[google.maps.ControlPosition.TOP_RIGHT].push(layerControlDiv);
    }
  }

  $.getJSON('chainsboundary.json?start='+ start.format('YYYY-MM-DD')+"&end="+ end.format('YYYY-MM-DD')+'&chain='+chain, function (json) {
    chains_dict = {}
    for (feature in json['features']){
      geojson = json['features'][feature]
      vendor = json['features'][feature]['properties']['vendor']
      delivery_population = json['features'][feature]['properties']['delivery_population']
      rx_num = json['features'][feature]['properties']['rx_num']
      
      chain_layer = new google.maps.Data({map: chains_map});
      chain_layer.addGeoJson(geojson);
      chains_dict[vendor] = {
        'layer':chain_layer,
        'vendor':vendor,
        'delivery_population':delivery_population,
        'rx_num':rx_num
      }

      zoom = chains_map.getZoom()
      chain_layer.setStyle({
        fillColor: vendor_data[vendor]['vendor_colour'],
        fillOpacity: 0.5,
        strokeColor: 1,
        strokeWeight: zoom/4,
        zIndex: 1/delivery_population
      });
    }
    //fit the map to the bounds
    chain_bounds = new google.maps.LatLngBounds();
    for (chain_layer in chains_dict){        
      chains_dict[chain_layer]['layer'].forEach(function(feature){
        feature.getGeometry().forEachLatLng(function(latlng){
            chain_bounds.extend(latlng)
            });
          });
    }
    chains_map.fitBounds(chain_bounds, 0)
 
    layers = new Set(Object.values(chains_dict).map(({vendor})=>vendor))
    layer_controller(layers, chains_map)
    setVisible('#loading', false);
  });
}